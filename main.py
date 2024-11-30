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
        """Make a request to the Companies House API with authentication."""
        try:
            self._rate_limit()
            logger.debug(f"Making request to {url} with params {params}")
            response = self.session.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            logger.debug(f"Response status: {response.status_code}")
            logger.debug(f"Response data: {json.dumps(data, indent=2)}")
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            if hasattr(e, 'response'):
                if e.response.status_code == 401:
                    logger.error("Authentication failed. Check your API key.")
                elif e.response.status_code == 429:
                    logger.error("Rate limit exceeded. Waiting before retrying...")
                    time.sleep(RATE_LIMIT_DELAY * 2)  # Wait longer on rate limit
                logger.error(f"Response status code: {e.response.status_code}")
                logger.error(f"Response text: {e.response.text}")
            return None

    def search_companies(self, sic_code):
        """Search for companies with specific SIC code."""
        all_companies = []
        start_index = 0
        items_per_page = 100
        max_results = 500  # Limit total results to avoid excessive API calls
        
        # First search for companies in general categories
        search_queries = [
            'waste management',  # For waste management companies
            'cleaning services',  # For cleaning companies
            'recycling',         # Additional relevant terms
            'facilities management'
        ]
        
        for search_query in search_queries:
            logger.info(f"Trying search query: {search_query}")
            start_index = 0
            
            while start_index < max_results:
                url = f"{BASE_URL}/search/companies"
                params = {
                    'q': search_query,
                    'items_per_page': items_per_page,
                    'start_index': start_index,
                    'restrictions': 'active'  # Only active companies
                }
                
                logger.info(f"Searching companies (start_index: {start_index})")
                data = self.make_request(url, params)
                
                if not data:
                    logger.error(f"Failed to get data for query {search_query}")
                    break
                    
                items = data.get('items', [])
                if not items:
                    logger.info(f"No more results for query {search_query}")
                    break
                    
                total_results = data.get('total_results', 0)
                logger.info(f"Found {len(items)} companies (total available: {total_results})")
                
                # Filter companies that have the specific SIC code
                filtered_items = []
                for company in items:
                    company_number = company.get('company_number')
                    if company_number:
                        details = self.get_company_details(company_number)
                        if details and 'sic_codes' in details:
                            sic_codes = [code.strip() for code in details.get('sic_codes', [])]
                            logger.debug(f"Company {company_number} SIC codes: {sic_codes}")
                            if any(code.startswith(str(sic_code)) for code in sic_codes):
                                filtered_items.append({
                                    'company_number': company_number,
                                    'company_name': company.get('title', ''),
                                    'company_status': details.get('company_status', ''),
                                    'incorporation_date': details.get('date_of_creation', ''),
                                    'sic_codes': sic_codes,
                                    'registered_office_address': details.get('registered_office_address', {}),
                                    'company_type': details.get('type', ''),
                                    'last_accounts_date': details.get('last_accounts', {}).get('made_up_to', '')
                                })
                                logger.info(f"Added company {company.get('title')} ({company_number}) with SIC codes {sic_codes}")
                
                all_companies.extend(filtered_items)
                
                if len(items) < items_per_page:
                    break
                    
                start_index += items_per_page
                
                # Respect rate limiting
                time.sleep(RATE_LIMIT_DELAY)
        
        # Remove duplicates based on company number
        unique_companies = {company['company_number']: company for company in all_companies}
        all_companies = list(unique_companies.values())
        
        logger.info(f"Total unique companies found: {len(all_companies)}")
        return all_companies

    def get_company_details(self, company_number):
        """Get detailed information about a company."""
        url = f"{BASE_URL}/company/{company_number}"
        logger.debug(f"Getting details for company {company_number}")
        
        data = self.make_request(url)
        if data:
            logger.debug(f"Got details for company {company_number}: {json.dumps(data, indent=2)}")
        else:
            logger.error(f"Failed to get details for company {company_number}")
        return data

    def get_company_officers(self, company_number):
        """Get officers of a company"""
        url = f"{BASE_URL}/company/{company_number}/officers"
        return self.make_request(url)

    def get_company_accounts(self, company_number):
        """Get company accounts information"""
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
            'number_of_active_directors_over_50',
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
                        
                        # Get detailed company information
                        details = self.get_company_details(company_number)
                        if not details:
                            logger.warning(f"Could not get details for company {company_number}")
                            continue
                        
                        # Check company status
                        if details.get('company_status', '').lower() != 'active':
                            logger.info(f"Skipping inactive company: {company_name}")
                            continue
                        
                        # Check for directors over minimum age
                        officers = self.get_company_officers(company_number)
                        active_directors_over_50 = 0
                        
                        if officers and 'items' in officers:
                            for officer in officers['items']:
                                if (officer.get('officer_role') == 'director' and 
                                    not officer.get('resigned_on')):
                                    age = self.calculate_age(officer.get('date_of_birth'))
                                    if age and age >= MIN_DIRECTOR_AGE:
                                        active_directors_over_50 += 1
                        
                        if active_directors_over_50 == 0:
                            logger.info(f"No active directors over 50 found for {company_name}")
                            continue
                        
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
                            'company_status': details.get('company_status', ''),
                            'incorporation_date': details.get('date_of_creation', ''),
                            'sic_codes': ', '.join(details.get('sic_codes', [])),
                            'registered_office_address': self._format_address(details.get('registered_office_address', {})),
                            'number_of_active_directors_over_50': active_directors_over_50,
                            'company_type': details.get('type', ''),
                            'companies_house_turnover': f"£{ch_turnover:,.2f}" if ch_turnover else 'Not available',
                            'hmrc_turnover': f"£{hmrc_turnover:,.2f}" if hmrc_turnover else 'Not available',
                            'last_accounts_date': (
                                details.get('last_accounts', {}).get('made_up_to', 'Not available')
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
