import os
import requests
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
import csv
import time
import base64
from urllib.parse import urlencode
import pytz

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.DEBUG)  # Change to DEBUG level
logger = logging.getLogger(__name__)

class HMRCClient:
    def __init__(self):
        self.api_key = os.getenv('HMRC_CLIENT_ID')
        self.client_secret = os.getenv('HMRC_CLIENT_SECRET')
        self.base_url = "https://api.service.hmrc.gov.uk"  
        
        if not self.api_key:
            raise ValueError("HMRC_CLIENT_ID not found in environment variables")
        
        if not self.client_secret:  # Add this check
            raise ValueError("HMRC_CLIENT_SECRET not found in environment variables")
        
        logger.info(f"Initialized HMRC client with API key: {self.api_key}")
        logger.debug(f"API Key: {self.api_key}")
        logger.debug(f"Client Secret: {self.client_secret}")
        logger.debug(f"Base URL: {self.base_url}")
        
        self.access_token = None
        self.token_expiry = None
        self.session = requests.Session()
        
        # Configure session for better error handling
        self.session.mount('https://', requests.adapters.HTTPAdapter(
            max_retries=3,
            pool_connections=10,
            pool_maxsize=10
        ))
        
        # Get initial auth token
        self.authenticate()

    def authenticate(self):
        """
        Authenticate with HMRC API and get access token
        """
        auth_url = f"{self.base_url}/oauth/token"
        
        # Check if we have a valid token
        current_time = datetime.now(pytz.utc)
        if self.access_token and self.token_expiry and current_time < self.token_expiry:
            return self.access_token

        # Create base64 encoded credentials
        credentials = base64.b64encode(f"{self.api_key}:{self.client_secret}".encode()).decode()
        
        headers = {
            'Authorization': f'Basic {credentials}',
            'Content-Type': "application/x-www-form-urlencoded",  # Update content type
            'Accept': 'application/vnd.hmrc.2.0+json'  # Add Accept header
        }
        
        # URL encode the form data properly
        data = {
            'client_id': self.api_key,
            'client_secret': self.client_secret,
            'grant_type': 'client_credentials'  # Ensure grant_type is included
        }
        encoded_data = urlencode(data)  # URL-encode the data
        
        logger.debug(f"Auth URL: {auth_url}")
        logger.debug(f"Encoded Credentials: {credentials}")
        logger.debug(f"Headers: {headers}")
        logger.debug(f"Encoded Data: {encoded_data}")
        
        try:
            logger.info(f"Attempting authentication with URL: {auth_url}")
            logger.info(f"Headers: Authorization: Basic ******, Content-Type: {headers['Content-Type']}")
            logger.info(f"Request data: {encoded_data}")  # Log the encoded data
            
            response = self.session.post(auth_url, headers=headers, data=encoded_data)  # Send encoded data
            
            logger.info(f"Response status code: {response.status_code}")
            logger.info(f"Response headers: {dict(response.headers)}")
            logger.debug(f"Response text: {response.text}")  # Add this line to log the response text
            
            if response.status_code != 200:
                logger.error(f"Response text: {response.text}")
            
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data['access_token']
            # Set token expiry (usually 4 hours from now)
            self.token_expiry = current_time + timedelta(seconds=token_data.get('expires_in', 14400))
            
            logger.info("Successfully authenticated with HMRC API")
            return self.access_token
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Authentication failed: {str(e)}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response: {e.response.text}")
            raise

    def make_request(self, url, method='get', data=None, params=None):
        """Make a request to the HMRC API"""
        max_retries = 3
        current_time = datetime.now(pytz.utc)
        timeout = 10  # Add a timeout of 10 seconds
        
        for attempt in range(max_retries):
            try:
                # Check if we need to authenticate
                if not self.access_token or current_time > self.token_expiry:
                    logger.info("Token might be expired, trying to re-authenticate...")
                    if not self.authenticate():
                        logger.error("Failed to authenticate with HMRC API")
                        return None
                
                headers = {
                    'Authorization': f'Bearer {self.access_token}',
                    'Accept': 'application/vnd.hmrc.2.0+json',
                    'Content-Type': 'application/json'
                }
                
                logger.info(f"Making {method.upper()} request to {url}")
                if method.lower() == 'get':
                    response = self.session.get(url, headers=headers, params=params, timeout=timeout)  # Add timeout
                else:
                    response = self.session.post(url, headers=headers, json=data, timeout=timeout)  # Add timeout
                
                response.raise_for_status()
                return response.json()
            
            except requests.exceptions.Timeout:
                logger.error(f"Request timed out on attempt {attempt + 1}")
                if attempt == max_retries - 1:
                    return None
                time.sleep(1)
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
        # Note: In production environment, this should return real data
        logger.info(f"Getting VAT info for company {company_number}")
        
        # For production environment, return real data
        url = f"{self.base_url}/organisations/vat/{company_number}/registration"
        response = self.make_request(url)
        if not response:
            logger.warning(f"Could not get VAT info for company {company_number}")
            return None
        
        vat_info = response
        return vat_info

    def get_company_turnover(self, vat_number):
        url = f"{self.base_url}/organisations/vat/{vat_number}/returns"
        headers = {
            "Authorization": f"Bearer {self.access_token}",  # Use access_token instead of api_key
            "Accept": "application/vnd.hmrc.1.0+json"
        }
        logger.debug(f"Fetching turnover for VAT number: {vat_number} with URL: {url}")
        
        try:
            response = self.session.get(url, headers=headers, timeout=10)  # Add timeout
            response.raise_for_status()
            
            if response.status_code == 200:
                data = response.json()
                # Assuming the turnover is part of the response data
                turnover = data.get("turnover")
                logger.debug(f"Turnover data received: {data}")
                return turnover
            elif response.status_code == 404:
                logger.error(f"Resource not found for VAT number {vat_number}. Status Code: {response.status_code}, Response: {response.text}")
                return None
            else:
                logger.error(f"Failed to fetch turnover for VAT number {vat_number}. Status Code: {response.status_code}, Response: {response.text}")
                return None
        except requests.exceptions.Timeout:
            logger.error(f"Request timed out for VAT number {vat_number}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for VAT number {vat_number}: {str(e)}")
            return None

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

def load_company_names_from_file(file_path):
    """Load company names from a file"""
    company_names = []
    with open(file_path, 'r') as file:
        for line in file:
            company_name = line.strip()
            if company_name:  # Only add non-empty names
                company_names.append(company_name)
    return company_names

def main():
    # Load company names from new-leads.txt
    vrn_list = load_company_names_from_file('/home/highcenburg/DEV/Clients/Data Retrieval/new-leads.txt')
    
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
