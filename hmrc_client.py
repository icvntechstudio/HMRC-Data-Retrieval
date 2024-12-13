import os
import requests
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
import csv
import time

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HMRCClient:
    def __init__(self):
        self.client_id = os.getenv('HMRC_API_KEY')
        self.client_secret = os.getenv('HMRC_SERVER_TOKEN')
        self.server_token = os.getenv('HMRC_SERVER_TOKEN')
        
        if not self.client_id:
            raise ValueError("HMRC_API_KEY not found in environment variables")
        if not self.client_secret:
            raise ValueError("HMRC_SERVER_TOKEN not found in environment variables")
            
        self.base_url = 'https://test-api.service.hmrc.gov.uk'
        self.access_token = None
        self.token_expiry = None
        self.session = requests.Session()
        
        # Configure session for better error handling
        self.session.mount('https://', requests.adapters.HTTPAdapter(
            max_retries=3,
            pool_connections=10,
            pool_maxsize=10
        ))
        
        logger.info(f"Initialized HMRC client with client ID: {'Present' if self.client_id else 'Missing'}")
        logger.info(f"Client secret: {'Present' if self.client_secret else 'Missing'}")
        logger.info("Using HMRC Test API environment")
        
        # Get initial auth token
        self.authenticate()

    def authenticate(self):
        """Authenticate with the HMRC API"""
        logger.info("Authenticating with HMRC API...")
        
        auth_url = f"{self.base_url}/oauth/token"
        
        # Prepare the authentication data
        auth_data = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'grant_type': 'client_credentials'
        }
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': f'Bearer {self.server_token}'
        }
        
        try:
            response = requests.post(auth_url, data=auth_data, headers=headers)
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data.get('access_token')
            expires_in = token_data.get('expires_in', 14400)  # Default 4 hours
            self.token_expiry = datetime.now() + timedelta(seconds=expires_in)
            
            logger.info("Successfully authenticated with HMRC API")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Authentication failed: {str(e)}")
            if hasattr(e.response, 'status_code'):
                logger.error(f"Response status code: {e.response.status_code}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response text: {e.response.text}")
            return False

    def make_request(self, url, method='get', data=None, params=None):
        """Make a request to the HMRC API"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # Check if we need to authenticate
                if not self.access_token or datetime.now() > self.token_expiry:
                    logger.info("Token might be expired, trying to re-authenticate...")
                    if not self.authenticate():
                        logger.error("Failed to authenticate with HMRC API")
                        return None
                
                headers = {
                    'Authorization': f'Bearer {self.access_token}',
                    'Accept': 'application/vnd.hmrc.1.0+json',
                    'Content-Type': 'application/json'
                }
                
                logger.info(f"Making {method.upper()} request to {url}")
                if method.lower() == 'get':
                    response = requests.get(url, headers=headers, params=params)
                else:
                    response = requests.post(url, headers=headers, json=data)
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP Error on attempt {attempt + 1}: {str(e)}")
                if e.response.status_code == 401:
                    logger.error(f"Response status code: {e.response.status_code}")
                    logger.error(f"Response text: {e.response.text}")
                    # Try to re-authenticate on next attempt
                    self.access_token = None
                    continue
                elif e.response.status_code == 429:  # Rate limit
                    wait_time = (attempt + 1) * 2
                    logger.warning(f"Rate limit hit, waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Response status code: {e.response.status_code}")
                    logger.error(f"Response text: {e.response.text}")
                    return None
            
            except Exception as e:
                logger.error(f"Request failed on attempt {attempt + 1}: {str(e)}")
                if attempt == max_retries - 1:
                    return None
                time.sleep(1)
        
        return None

    def get_vat_info(self, company_number):
        """Get VAT information for a company"""
        # Note: In test environment, this might not return real data
        logger.info(f"Getting VAT info for company {company_number}")
        
        # For test environment, return simulated data
        return {
            'vatNumber': f"GB{company_number}",
            'registrationDate': '2020-01-01'
        }

    def get_company_turnover(self, vat_number):
        """Get company turnover from VAT returns"""
        # Note: In test environment, this might not return real data
        logger.info(f"Getting turnover for VAT number {vat_number}")
        
        # For test environment, return simulated data with a wider range
        import random
        
        # Generate turnovers across different ranges with weighted probabilities
        ranges = [
            (1_000_000, 10_000_000, 0.25),     # 25% chance of £1M-£10M
            (10_000_000, 50_000_000, 0.35),    # 35% chance of £10M-£50M
            (50_000_000, 250_000_000, 0.25),   # 25% chance of £50M-£250M
            (250_000_000, 1_000_000_000, 0.15) # 15% chance of £250M-£1B
        ]
        
        # Choose a range based on probabilities
        range_choice = random.random()
        cumulative_prob = 0
        for min_val, max_val, prob in ranges:
            cumulative_prob += prob
            if range_choice <= cumulative_prob:
                return random.uniform(min_val, max_val)
        
        # Fallback (shouldn't reach here due to probabilities summing to 1)
        return random.uniform(10_000_000, 50_000_000)

    def process_companies(self, vrn_list):
        """Process list of companies by VAT registration numbers"""
        logger.info(f"Starting to process {len(vrn_list)} companies")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'hmrc_filtered_companies_{timestamp}.csv'
        logger.info(f"Will save results to: {output_file}")
        
        fieldnames = [
            'vrn',
            'company_name',
            'annual_turnover',
            'vat_status',
            'last_return_date'
        ]
        
        companies_processed = 0
        companies_saved = 0
        
        try:
            with open(output_file, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                logger.info("Created CSV file and wrote header")
                
                for vrn in vrn_list:
                    logger.info(f"Processing company with VRN: {vrn}")
                    companies_processed += 1
                    
                    # Get VAT information
                    vat_info = self.get_vat_info(vrn)
                    if not vat_info:
                        logger.warning(f"Could not get VAT info for VRN {vrn}")
                        continue
                    
                    # Get turnover
                    turnover = self.get_company_turnover(vrn)
                    if turnover is None:
                        logger.warning(f"Could not get turnover for VRN {vrn}")
                        continue
                    
                    # Only include companies with turnover >= £1M
                    if turnover < 1000000:
                        logger.info(f"Company with VRN {vrn} turnover (£{turnover:,.2f}) below threshold")
                        continue
                    
                    company_data = {
                        'vrn': vrn,
                        'company_name': vat_info.get('tradingName', 'Unknown'),
                        'annual_turnover': f"£{turnover:,.2f}",
                        'vat_status': vat_info.get('vatStatus', 'Unknown'),
                        'last_return_date': vat_info.get('lastReturnDate', 'Unknown')
                    }
                    
                    logger.info(f"Writing data for VRN {vrn}: {company_data}")
                    writer.writerow(company_data)
                    companies_saved += 1
                    logger.info(f"Successfully wrote data for VRN {vrn}")
                    
                    # Respect rate limiting
                    time.sleep(0.5)  # Adjust as needed based on HMRC rate limits
            
            logger.info(f"Processing complete. Processed {companies_processed} companies, saved {companies_saved} to CSV")
            return output_file
            
        except IOError as e:
            logger.error(f"Error writing to CSV file: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error during processing: {str(e)}")
            return None

def main():
    # You'll need to provide a list of VAT Registration Numbers to process
    vrn_list = [
        "123456789",  # Example VRN
        "987654321"   # Example VRN
    ]
    
    logger.info("Starting HMRC data retrieval script")
    client = HMRCClient()
    
    try:
        output_file = client.process_companies(vrn_list)
        
        if output_file:
            logger.info(f"Data has been saved to {output_file}")
            print(f"Data has been saved to {output_file}")
        else:
            logger.error("No data was saved")
            print("No data was saved")
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
