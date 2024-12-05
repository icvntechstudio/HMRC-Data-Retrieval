import os
import requests
import logging
import urllib.parse
import json
import csv
from datetime import datetime
import time
from hmrc_client import HMRCClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
COMPANIES_API_KEY = os.getenv('COMPANIES_API_KEY')
BASE_URL = 'https://api.companieshouse.gov.uk'
RATE_LIMIT_DELAY = 0.6  # Minimum delay between requests (in seconds)
MIN_DIRECTOR_AGE = 50  # Minimum age for directors
MIN_TURNOVER = 1_000_000  # £1 million minimum turnover
MAX_TURNOVER = 1_000_000_000  # £1 billion maximum turnover

class CompaniesHouseClient:
    def __init__(self):
        """Initialize the Companies House client."""
        self.api_key = os.getenv('COMPANIES_API_KEY')
        if not self.api_key:
            raise ValueError("COMPANIES_API_KEY not found in environment variables")
            
        # Set up session with authentication
        self.session = requests.Session()
        self.session.auth = (self.api_key, '')
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'CompanyDataRetrieval/1.0'
        })
        
        # Configure rate limiting
        self.last_request_time = time.time()
        self.request_times = []  # Keep track of request timestamps
        self.max_requests_per_minute = 500  # Conservative limit
        self.min_request_interval = 0.15  # Minimum time between requests in seconds
        
        self.hmrc_client = HMRCClient()
        logger.info("Initialized Companies House client")

    def _rate_limit(self):
        """Implement rate limiting for API requests."""
        current_time = time.time()
        
        # Remove request timestamps older than 1 minute
        self.request_times = [t for t in self.request_times if current_time - t <= 60]
        
        # If we've made too many requests in the last minute, wait
        if len(self.request_times) >= self.max_requests_per_minute:
            sleep_time = 60 - (current_time - self.request_times[0])
            if sleep_time > 0:
                logger.info(f"Rate limit approaching, waiting {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
                self.request_times = []  # Reset after waiting
        
        # Ensure minimum interval between requests
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last_request)
        
        self.last_request_time = time.time()
        self.request_times.append(self.last_request_time)

    def make_request(self, url, params=None):
        """Make a request to the Companies House API with retry logic"""
        max_retries = 3
        base_delay = 2  # Base delay for exponential backoff
        
        for attempt in range(max_retries):
            try:
                self._rate_limit()  # Apply rate limiting
                
                logger.debug(f"Making request to {url}")
                response = self.session.get(
                    url,
                    params=params,
                    timeout=30
                )
                
                if response.status_code == 429:  # Rate limit exceeded
                    retry_after = int(response.headers.get('Retry-After', base_delay * (2 ** attempt)))
                    logger.warning(f"Rate limit exceeded. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {str(e)}")
                if attempt < max_retries - 1:
                    sleep_time = base_delay * (2 ** attempt)
                    logger.info(f"Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                else:
                    return None
        
        return None

    def search_companies(self, sic_code):
        """Search for companies with specific SIC code"""
        companies = []
        items_per_page = 100
        max_results = 20000
        processed_companies = set()
        
        # Search terms optimized for each SIC code
        search_terms = {
            # General cleaning
            '81210': [f'"{sic_code}" cleaning'],
            '81200': [f'"{sic_code}" cleaning'],
            
            # Specialized cleaning
            '81220': [f'"{sic_code}" cleaning'],
            '81221': [f'"{sic_code}" window cleaning'],
            '81222': [f'"{sic_code}" specialized cleaning'],
            '81223': [f'"{sic_code}" chimney cleaning'],
            '81229': [f'"{sic_code}" specialized cleaning'],
            
            # Other cleaning
            '81290': [f'"{sic_code}" cleaning'],
            '81291': [f'"{sic_code}" disinfecting'],
            '81299': [f'"{sic_code}" cleaning'],
            
            # Additional services
            '81300': [f'"{sic_code}" landscaping'],
            '82990': [f'"{sic_code}" cleaning'],
            
            # Waste management
            '38110': [f'"{sic_code}" waste'],
            '38210': [f'"{sic_code}" waste treatment'],
            '38220': [f'"{sic_code}" hazardous waste'],
            '38320': [f'"{sic_code}" recycling']
        }
        
        terms = search_terms.get(sic_code, [f'"{sic_code}"'])
        
        for term in terms:
            logger.info(f"Searching with term: {term}")
            start_index = 0
            
            while start_index < max_results:
                try:
                    params = {
                        'q': term,
                        'items_per_page': items_per_page,
                        'start_index': start_index,
                        'restrictions': 'active'
                    }
                    
                    response_data = self.make_request(f"{BASE_URL}/search/companies", params)
                    
                    if not response_data or 'items' not in response_data:
                        break
                    
                    items = response_data['items']
                    if not items:
                        break
                    
                    total_items = response_data.get('total_results', 0)
                    logger.info(f"Processing {len(items)} companies from index {start_index}. Total available: {total_items}")
                    
                    # Process companies in batches
                    for company in items:
                        company_number = company.get('company_number')
                        
                        if not company_number or company_number in processed_companies:
                            continue
                        
                        # Get basic company details first
                        company_details = {
                            'company_number': company_number,
                            'company_name': company.get('company_name', ''),
                            'company_status': company.get('company_status', ''),
                            'date_of_creation': company.get('date_of_creation', ''),
                            'company_type': company.get('type', '')
                        }
                        
                        # Only get full details if basic criteria are met
                        if company_details['company_status'].lower() == 'active':
                            full_details = self.get_company_details(company_number)
                            if full_details:
                                company_details.update(full_details)
                                companies.append(company_details)
                                processed_companies.add(company_number)
                                logger.debug(f"Found matching company: {company_details['company_name']}")
                    
                    start_index += len(items)
                    if start_index >= min(total_items, max_results):
                        break
                    
                except Exception as e:
                    logger.error(f"Error processing search term {term} at index {start_index}: {str(e)}")
                    break
        
        logger.info(f"Found {len(companies)} unique companies for SIC code {sic_code}")
        return companies

    def get_company_details(self, company_number):
        """Get detailed information about a company"""
        if not company_number:
            return None
            
        url = f"{BASE_URL}/company/{company_number}"
        data = self.make_request(url)
        
        if data:
            # Add the company number to the data if not present
            data['company_number'] = company_number
            
            # Clean up the company name
            if 'company_name' not in data and 'title' in data:
                data['company_name'] = data['title']
            
            # Ensure SIC codes are present
            if 'sic_codes' not in data:
                data['sic_codes'] = []
                
        return data

    def get_company_officers(self, company_number):
        """Get officers of a company"""
        if not company_number:
            return None
            
        url = f"{BASE_URL}/company/{company_number}/officers"
        params = {
            'items_per_page': 100,
            'status': 'active'  # Only get active officers
        }
        return self.make_request(url, params)

    def get_company_accounts(self, company_number):
        """Get company accounts information"""
        if not company_number:
            return None
            
        url = f"{BASE_URL}/company/{company_number}/filing-history"
        data = self.make_request(url)
        
        if not data or 'items' not in data:
            logger.warning(f"No filing history found for company {company_number}")
            return None
            
        # Look for the most recent full accounts
        for filing in data.get('items', []):
            if filing.get('category') in ['accounts', 'accounts-with-accounts-type-full', 'accounts-with-accounts-type-small']:
                accounts_data = filing.get('data', {})
                # Try different possible turnover fields
                turnover_fields = ['turnover', 'revenue', 'total_turnover', 'uk_turnover']
                for field in turnover_fields:
                    if field in accounts_data:
                        try:
                            turnover_str = str(accounts_data[field])
                            # Add debug logging
                            logger.debug(f"Raw turnover value for company {company_number}: {turnover_str}")
                            
                            # Handle different formats
                            if isinstance(turnover_str, (int, float)):
                                value = float(turnover_str)
                                logger.debug(f"Direct numeric turnover for company {company_number}: {value}")
                                return float(turnover_str)
                            
                            # Remove currency symbols and commas
                            turnover_str = turnover_str.replace('£', '').replace(',', '').strip().lower()
                            logger.debug(f"Cleaned turnover string: {turnover_str}")
                            
                            # Handle ranges like "1000000-5000000"
                            if '-' in turnover_str:
                                lower, upper = map(str.strip, turnover_str.split('-'))
                                try:
                                    # Try to get both bounds
                                    lower_val = float(lower)
                                    upper_val = float(upper)
                                    value = upper_val  # Use upper value instead of max
                                    logger.debug(f"Range turnover for company {company_number}: {lower_val}-{upper_val}, using {value}")
                                    return value
                                except ValueError:
                                    # If upper bound fails, use lower bound
                                    value = float(lower)
                                    logger.debug(f"Using lower bound for company {company_number}: {value}")
                                    return value
                            
                            # Handle text-based ranges
                            turnover_bands = {
                                # Exact ranges - use upper bound instead of middle value
                                '1m-5m': 5_000_000,  # Use upper value
                                '5m-10m': 10_000_000,  # Use upper value
                                '10m-50m': 50_000_000,  # Use upper value
                                '50m-100m': 100_000_000,  # Use upper value
                                '100m-500m': 500_000_000,  # Use upper value
                                '500m-1b': 1_000_000_000,  # Use upper value
                                
                                # 'Over X' ranges - use higher estimates
                                'over 500m': min(900_000_000, MAX_TURNOVER),
                                'over 250m': min(500_000_000, MAX_TURNOVER),
                                'over 100m': min(250_000_000, MAX_TURNOVER),
                                'over 50m': min(100_000_000, MAX_TURNOVER),
                                'over 25m': min(50_000_000, MAX_TURNOVER),
                                'over 10m': min(25_000_000, MAX_TURNOVER),
                                'over 5m': min(10_000_000, MAX_TURNOVER),
                                'over 2m': min(5_000_000, MAX_TURNOVER),
                                'over 1m': min(2_500_000, MAX_TURNOVER),
                            }
                            
                            # Add more variations of number formats
                            number_suffixes = {
                                'k': 1_000,
                                'm': 1_000_000,
                                'b': 1_000_000_000
                            }
                            
                            # Try to parse numbers with suffixes (e.g., "5m", "2.5m", "1.2b")
                            for suffix, multiplier in number_suffixes.items():
                                if turnover_str.endswith(suffix):
                                    try:
                                        base_value = float(turnover_str[:-1])
                                        value = base_value * multiplier
                                        if MIN_TURNOVER <= value <= MAX_TURNOVER:
                                            logger.debug(f"Parsed suffixed number for company {company_number}: {turnover_str} -> {value}")
                                            return value
                                    except ValueError:
                                        continue
                            
                            for band, value in turnover_bands.items():
                                if band.lower() in turnover_str:
                                    # Ensure the value is within our limits
                                    if MIN_TURNOVER <= value <= MAX_TURNOVER:
                                        logger.debug(f"Matched turnover band for company {company_number}: {band} -> {value}")
                                        return value
                                    else:
                                        logger.info(f"Turnover value {value} for band {band} outside allowed range for company {company_number}")
                                        return None
                            
                            # Try direct conversion
                            try:
                                value = float(turnover_str)
                                # Check if the direct value is within our limits
                                if MIN_TURNOVER <= value <= MAX_TURNOVER:
                                    logger.debug(f"Direct conversion turnover for company {company_number}: {value}")
                                    return value
                                else:
                                    logger.info(f"Direct turnover value {value} outside allowed range for company {company_number}")
                                    return None
                            except ValueError:
                                logger.debug(f"Could not parse turnover value '{turnover_str}' for company {company_number}")
                                return None
                            
                        except (ValueError, AttributeError) as e:
                            logger.warning(f"Could not parse turnover value '{accounts_data[field]}' for company {company_number}: {e}")
                            continue
        
        logger.warning(f"No turnover information found in filing history for company {company_number}")
        return None

    def process_companies(self):
        """Process companies and save to CSV"""
        # Define SIC codes for cleaning and waste management
        sic_codes = {
            "Cleaning": [
                '81210',  # General cleaning of buildings
                '81229',  # Other specialized cleaning activities
                '81220',  # Other building and industrial cleaning activities
                '81222',  # Specialized cleaning activities
                '81221',  # Window cleaning services
                '81223',  # Chimney cleaning services
                '81299',  # Other cleaning services n.e.c.
                '81290',  # Other cleaning activities
                '81291',  # Disinfecting and exterminating services
                '81200',  # General cleaning activities
                '81300',  # Landscaping activities
                '82990',  # Other business support activities
            ],
            "Waste Management": [
                '38110',  # Collection of non-hazardous waste
                '38320',  # Recovery of sorted materials
                '38220',  # Treatment and disposal of hazardous waste
                '38210',  # Treatment and disposal of non-hazardous waste
            ]
        }
        
        # Create output file with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'filtered_companies_{timestamp}.csv'
        
        # Define CSV fields
        fieldnames = [
            'company_number', 'company_name', 'company_status',
            'incorporation_date', 'sic_codes', 'registered_office_address',
            'active_directors_over_50', 'company_type', 'companies_house_turnover',
            'hmrc_turnover', 'last_accounts_date', 'category', 'vat_number'
        ]
        
        processed_count = 0
        saved_count = 0
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for category, codes in sic_codes.items():
                logger.info(f"Processing {category} companies...")
                
                for sic_code in codes:
                    logger.info(f"Searching for companies with SIC code {sic_code}")
                    companies = self.search_companies(sic_code)
                    
                    if not companies:
                        logger.warning(f"No companies found for SIC code {sic_code}")
                        continue
                    
                    # Process companies in batches
                    batch_size = 50  # Reduced batch size for better handling
                    for i in range(0, len(companies), batch_size):
                        batch = companies[i:i + batch_size]
                        logger.info(f"Processing batch {i//batch_size + 1} of {len(companies)//batch_size + 1}")
                        
                        for company in batch:
                            processed_count += 1
                            company_number = company.get('company_number')
                            company_name = company.get('company_name', 'Unknown')
                            
                            try:
                                # Get turnover information
                                ch_turnover = self.get_company_accounts(company_number)
                                if ch_turnover is None:
                                    logger.debug(f"No Companies House turnover data for {company_name} ({company_number})")
                                else:
                                    logger.debug(f"Companies House turnover for {company_name}: £{ch_turnover:,.2f}")
                                
                                # Get VAT number and HMRC turnover
                                vat_info = self.hmrc_client.get_vat_info(company_number)
                                hmrc_turnover = None
                                vat_number = None
                                
                                if vat_info:
                                    vat_number = vat_info.get('vatNumber')
                                    if vat_number:
                                        hmrc_turnover = self.hmrc_client.get_company_turnover(vat_number)
                                        if hmrc_turnover:
                                            logger.debug(f"HMRC turnover for {company_name}: £{hmrc_turnover:,.2f}")
                                
                                # Check if either turnover meets our criteria (£1M to £1B)
                                turnover_ok = False
                                
                                # Check Companies House turnover
                                if ch_turnover and MIN_TURNOVER <= ch_turnover <= MAX_TURNOVER:
                                    turnover_ok = True
                                    logger.debug(f"Company {company_name} meets turnover criteria from Companies House: £{ch_turnover:,.2f}")
                                # Check HMRC turnover if Companies House turnover wasn't sufficient
                                elif hmrc_turnover and MIN_TURNOVER <= hmrc_turnover <= MAX_TURNOVER:
                                    turnover_ok = True
                                    logger.debug(f"Company {company_name} meets turnover criteria from HMRC: £{hmrc_turnover:,.2f}")
                                
                                # Only proceed if we have a valid turnover between £1M and £1B
                                if not turnover_ok:
                                    logger.debug(f"Skipping {company_name} - No valid turnover between £{MIN_TURNOVER:,.0f} and £{MAX_TURNOVER:,.0f}")
                                    continue
                                
                                # Get officers and check ages
                                officers_data = self.get_company_officers(company_number)
                                directors_over_50_info = []
                                total_directors = 0
                                
                                if officers_data and 'items' in officers_data:
                                    for officer in officers_data['items']:
                                        if officer.get('officer_role', '').lower() == 'director':
                                            total_directors += 1
                                            date_of_birth = officer.get('date_of_birth')
                                            if date_of_birth:
                                                age = self.calculate_age(date_of_birth)
                                                if age:
                                                    if age >= MIN_DIRECTOR_AGE:
                                                        # Get director's name
                                                        name_parts = []
                                                        if officer.get('name_elements'):
                                                            title = officer['name_elements'].get('title', '')
                                                            forename = officer['name_elements'].get('forename', '')
                                                            surname = officer['name_elements'].get('surname', '')
                                                            name_parts = [p for p in [title, forename, surname] if p]
                                                        
                                                        # Use name from name_elements if available, otherwise fallback to name field
                                                        director_name = ' '.join(name_parts) if name_parts else officer.get('name', 'Unknown')
                                                        directors_over_50_info.append(f"{director_name} (Age: {age})")
                                                        logger.debug(f"Found director over {MIN_DIRECTOR_AGE} for {company_name}: {director_name} (Age: {age})")
                                    
                                    if total_directors == 0:
                                        logger.debug(f"Skipping {company_name} - No directors found")
                                    else:
                                        logger.debug(f"Company {company_name} has {total_directors} directors, {len(directors_over_50_info)} are over {MIN_DIRECTOR_AGE}")
                                else:
                                    logger.debug(f"Skipping {company_name} - No officers data available")

                                # Skip if no directors over 50
                                if not directors_over_50_info:
                                    logger.debug(f"Skipping {company_name} - No directors aged {MIN_DIRECTOR_AGE}+")
                                    continue
                                
                                # Save companies that have £1M+ turnover and directors aged 50+
                                company_data = {
                                    'company_number': company_number,
                                    'company_name': company_name,
                                    'company_status': company.get('company_status', ''),
                                    'incorporation_date': company.get('date_of_creation', ''),
                                    'sic_codes': ', '.join(company.get('sic_codes', [])),
                                    'registered_office_address': self._format_address(company.get('registered_office_address', {})),
                                    'active_directors_over_50': '; '.join(directors_over_50_info),
                                    'company_type': company.get('type', ''),
                                    'companies_house_turnover': f"£{ch_turnover:,.2f}" if ch_turnover else 'Not available',
                                    'hmrc_turnover': f"£{hmrc_turnover:,.2f}" if hmrc_turnover else 'Not available',
                                    'last_accounts_date': (
                                        company.get('last_accounts', {}).get('made_up_to', 'Not available')
                                    ),
                                    'category': category,
                                    'vat_number': vat_number or 'Not available'
                                }
                                
                                writer.writerow(company_data)
                                csvfile.flush()  # Force write to disk
                                saved_count += 1
                                logger.info(f"Saved data for company {company_name}")
                                
                            except Exception as e:
                                logger.error(f"Error processing company {company_name}: {str(e)}")
                                continue
                            
                            # Add a small delay between companies
                            time.sleep(RATE_LIMIT_DELAY)
                        
                        logger.info(f"Completed batch. Total processed: {processed_count}, Total saved: {saved_count}")
                    
                    logger.info(f"Completed SIC code {sic_code}. Total processed: {processed_count}, Total saved: {saved_count}")
                
                logger.info(f"Completed category {category}. Total processed: {processed_count}, Total saved: {saved_count}")
        
        logger.info(f"Processing complete. Processed {processed_count} companies, saved {saved_count} to CSV")
        return output_file

    def _format_address(self, address_dict):
        """Format address dictionary into a string"""
        if not address_dict:
            return ''
        
        address_parts = [
            address_dict.get('address_line_1', ''),
            address_dict.get('address_line_2', ''),
            address_dict.get('locality', ''),
            address_dict.get('region', ''),
            address_dict.get('postal_code', ''),
            address_dict.get('country', '')
        ]
        return ', '.join(part for part in address_parts if part)

    def calculate_age(self, date_of_birth):
        """Calculate age from date of birth dictionary"""
        if not date_of_birth or 'year' not in date_of_birth:
            return None
            
        try:
            # Create a date object using year and month (if available)
            year = int(date_of_birth['year'])
            month = int(date_of_birth.get('month', 1))
            day = 1  # Default to first of the month
            
            birth_date = datetime(year, month, day)
            today = datetime.now()
            
            age = today.year - birth_date.year
            
            # Adjust age if birthday hasn't occurred this year
            if today.month < birth_date.month:
                age -= 1
                
            return age
        except (ValueError, TypeError):
            logger.error(f"Error calculating age for date of birth: {date_of_birth}")
            return None

def main():
    try:
        logger.info("Starting company data retrieval process")
        client = CompaniesHouseClient()
        output_file = client.process_companies()
        
        if output_file:
            logger.info(f"Data has been saved to {output_file}")
        else:
            logger.error("No data was saved")
            
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()
