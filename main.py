import os
import requests
import logging
import base64
from dotenv import load_dotenv
import urllib.parse
import json
import csv
from datetime import datetime
import time
from hmrc_client import HMRCClient

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
BASE_URL = 'https://api.company-information.service.gov.uk'
RATE_LIMIT_DELAY = 0.6  # Minimum delay between requests (in seconds)
MIN_DIRECTOR_AGE = 50  # Minimum age for directors
MIN_TURNOVER = 1000000  # Minimum turnover in pounds

class CompaniesHouseClient:
    def __init__(self):
        """Initialize the Companies House client."""
        self.api_key = os.getenv('COMPANIES_API_KEY')
        if not self.api_key:
            raise ValueError("COMPANIES_API_KEY not found in environment variables")
            
        # Set up session with authentication
        self.session = requests.Session()
        self.session.auth = (self.api_key, '')  # Companies House uses the API key as username and empty password
        
        # Configure session for better error handling
        self.session.hooks = {
            'response': lambda r, *args, **kwargs: r.raise_for_status()
        }
        
        # Initialize rate limiting
        self.last_request_time = 0
        self.requests_per_second = 0.5  # Max 30 requests per minute
        
        self.hmrc_client = HMRCClient()
        logger.info("Initialized Companies House client")

    def _rate_limit(self):
        """Implement rate limiting for API requests."""
        current_time = time.time()
        min_interval = 1.0 / self.requests_per_second
        
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < min_interval:
            sleep_time = min_interval - time_since_last_request
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()

    def make_request(self, url, params=None):
        """Make a request to the Companies House API with retry logic"""
        max_retries = 3
        retry_delay = 5  # increased delay between retries
        
        for attempt in range(max_retries):
            try:
                time.sleep(retry_delay)  # Always wait between requests
                logger.info(f"Making request to {url} (Attempt {attempt + 1}/{max_retries})")
                response = requests.get(
                    url,
                    params=params,
                    auth=(self.api_key, ''),
                    timeout=30,
                    headers={'Accept': 'application/json'}
                )
                response.raise_for_status()
                return response.json()
            except Exception as e:
                logger.error(f"Request failed: {str(e)}")
                if attempt == max_retries - 1:
                    return None
        return None

    def search_companies(self, sic_code):
        """Search for companies with specific SIC code"""
        companies = []
        items_per_page = 100  # Maximum allowed by API
        max_pages = 50  # Increased to get more results
        
        # Search terms that are likely to find companies in our target sectors
        search_terms = {
            '81210': ['cleaning', 'building cleaning', 'office cleaning', 'commercial cleaning', 'facilities'],
            '81220': ['industrial cleaning', 'specialist cleaning', 'commercial cleaning', 'building'],
            '81290': ['cleaning services', 'commercial cleaning', 'specialist', 'facilities'],
            '38110': ['waste collection', 'waste management', 'recycling', 'environmental'],
            '38120': ['hazardous waste', 'chemical waste', 'waste management', 'environmental'],
            '38210': ['waste treatment', 'waste disposal', 'recycling', 'environmental'],
            '38220': ['hazardous waste treatment', 'waste management', 'environmental'],
            '38230': ['waste recycling', 'materials recovery', 'recycling', 'environmental']
        }
        
        terms = search_terms.get(sic_code, [sic_code])
        processed_companies = set()  # Track processed companies to avoid duplicates
        
        for term in terms:
            logger.info(f"Searching with term: {term}")
            start_index = 0
            
            for page in range(max_pages):
                params = {
                    'q': term,
                    'items_per_page': items_per_page,
                    'start_index': start_index,
                    'restrictions': 'active'
                }
                
                url = f"{BASE_URL}/search/companies"
                response_data = self.make_request(url, params)
                
                if not response_data or 'items' not in response_data:
                    logger.warning(f"No results found for search term: {term} on page {page + 1}")
                    break
                
                items = response_data['items']
                if not items:
                    break
                
                logger.info(f"Processing {len(items)} companies from page {page + 1}")
                
                # Filter companies by SIC code
                for company in items:
                    company_number = company.get('company_number')
                    
                    # Skip if we've already processed this company
                    if company_number in processed_companies:
                        continue
                    
                    # Get full company details to check SIC codes
                    company_details = self.get_company_details(company_number)
                    if company_details and 'sic_codes' in company_details:
                        company_sic_codes = company_details.get('sic_codes', [])
                        
                        # Check if any SIC code matches our target
                        if any(code.startswith(sic_code) for code in company_sic_codes):
                            companies.append(company_details)
                            processed_companies.add(company_number)
                            logger.info(f"Found matching company: {company_details.get('company_name')} with SIC codes {company_sic_codes}")
                
                # Move to next page
                start_index += items_per_page
                
                # If we got fewer items than requested, we've reached the end
                if len(items) < items_per_page:
                    break
                
                # Add a delay between pages to respect rate limits
                time.sleep(3)
            
            # Add a delay between search terms
            time.sleep(5)
        
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
                            # Remove currency symbols and commas, convert to float
                            turnover_str = accounts_data[field].replace('£', '').replace(',', '')
                            # Handle ranges like "1000000-5000000"
                            if '-' in turnover_str:
                                lower, upper = turnover_str.split('-')
                                return float(lower.strip())  # Use lower bound conservatively
                            return float(turnover_str)
                        except (ValueError, AttributeError) as e:
                            logger.warning(f"Could not parse turnover value '{accounts_data[field]}' for company {company_number}: {e}")
                            continue
        
        logger.warning(f"No turnover information found in filing history for company {company_number}")
        return None

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

    def process_companies(self):
        """Process companies and save to CSV"""
        # Define SIC codes for cleaning and waste management
        sic_codes = {
            "Cleaning": [
                '81210',  # General cleaning of buildings
                '81220',  # Other building and industrial cleaning activities
                '81290',  # Other cleaning activities
            ],
            "Waste Management": [
                '38110',  # Collection of non-hazardous waste
                '38120',  # Collection of hazardous waste
                '38210',  # Treatment and disposal of non-hazardous waste
                '38220',  # Treatment and disposal of hazardous waste
                '38230',  # Recovery of sorted materials
            ]
        }
        
        # Create output file with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'filtered_companies_{timestamp}.csv'
        
        # Define CSV fields
        fieldnames = [
            'company_number',
            'company_name',
            'company_status',
            'incorporation_date',
            'sic_codes',
            'registered_office_address',
            'active_directors_over_50',
            'company_type',
            'companies_house_turnover',
            'hmrc_turnover',
            'last_accounts_date',
            'category',
            'vat_number'
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
                    
                    for company in companies:
                        processed_count += 1
                        company_number = company.get('company_number')
                        company_name = company.get('company_name', 'Unknown')
                        
                        if not company_number:
                            logger.warning(f"Skipping company with no number: {company}")
                            continue
                        
                        logger.info(f"Processing company: {company_name} ({company_number})")
                        
                        # Check company status
                        if company.get('company_status', '').lower() != 'active':
                            logger.info(f"Skipping inactive company: {company_name}")
                            continue
                        
                        # Get directors and their ages
                        officers = self.get_company_officers(company_number)
                        eligible_directors = []
                        
                        if officers and 'items' in officers:
                            for officer in officers['items']:
                                if (officer.get('officer_role') == 'director' and 
                                    not officer.get('resigned_on')):
                                    age = self.calculate_age(officer.get('date_of_birth'))
                                    if age and age >= MIN_DIRECTOR_AGE:
                                        eligible_directors.append({
                                            'name': officer.get('name', ''),
                                            'age': age,
                                            'appointed_on': officer.get('appointed_on', '')
                                        })
                        
                        if not eligible_directors:
                            logger.info(f"No active directors over 50 found for {company_name}")
                            continue
                        
                        # Format director information
                        directors_info = '; '.join([
                            f"{d['name']} (Age: {d['age']}, Appointed: {d['appointed_on']})"
                            for d in eligible_directors
                        ])
                        
                        # Get turnover information from Companies House
                        ch_turnover = self.get_company_accounts(company_number)
                        
                        # Get VAT number and HMRC turnover
                        vat_info = self.hmrc_client.get_vat_info(company_number)
                        hmrc_turnover = None
                        vat_number = None
                        
                        if vat_info:
                            vat_number = vat_info.get('vatNumber')
                            if vat_number:
                                hmrc_turnover = self.hmrc_client.get_company_turnover(vat_number)
                        
                        # Check if either turnover meets the minimum requirement
                        meets_turnover = (
                            (ch_turnover and ch_turnover >= MIN_TURNOVER) or
                            (hmrc_turnover and hmrc_turnover >= MIN_TURNOVER)
                        )
                        
                        if not meets_turnover:
                            logger.info(f"Company {company_name} does not meet turnover requirements")
                            continue
                        
                        # Save company data
                        company_data = {
                            'company_number': company_number,
                            'company_name': company_name,
                            'company_status': company.get('company_status', ''),
                            'incorporation_date': company.get('date_of_creation', ''),
                            'sic_codes': ', '.join(company.get('sic_codes', [])),
                            'registered_office_address': self._format_address(company.get('registered_office_address', {})),
                            'active_directors_over_50': directors_info,
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
                        saved_count += 1
                        logger.info(f"Saved data for company {company_name}")
        
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
