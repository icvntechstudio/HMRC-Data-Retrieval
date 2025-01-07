import os
import requests
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
import csv
import time
import base64
from urllib.parse import urlencode

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HMRCClient:
    def __init__(self):
        self.client_id = os.getenv('HMRC_CLIENT_ID')
        self.client_secret = os.getenv('HMRC_CLIENT_SECRET')
        
        if not self.client_id:
            raise ValueError("HMRC_CLIENT_ID not found in environment variables")
        if not self.client_secret:
            raise ValueError("HMRC_CLIENT_SECRET not found in environment variables")
        
        logger.info("Initialized HMRC client with client ID: Present")
        logger.info("Client secret: Present")
        
        # Use sandbox environment
        self.base_url = 'https://test-api.service.hmrc.gov.uk'
        logger.info("Using HMRC Test API environment")
        
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
        current_time = datetime.fromisoformat("2025-01-07T20:08:58+08:00")
        if self.access_token and self.token_expiry and current_time < self.token_expiry:
            return self.access_token

        # Create base64 encoded credentials
        credentials = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
        
        headers = {
            'Authorization': f'Basic {credentials}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        # URL encode the form data properly
        data = urlencode({
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'grant_type': 'client_credentials'
        })
        
        try:
            logger.info(f"Attempting authentication with URL: {auth_url}")
            logger.info(f"Headers: Authorization: Basic ******, Content-Type: {headers['Content-Type']}")
            logger.info("Request data: client_id, client_secret, and grant_type present")
            
            response = self.session.post(auth_url, headers=headers, data=data)
            
            logger.info(f"Response status code: {response.status_code}")
            logger.info(f"Response headers: {dict(response.headers)}")
            
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
        current_time = datetime.fromisoformat("2025-01-07T20:10:23+08:00")
        
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
                    'Accept': 'application/vnd.hmrc.1.0+json',
                    'Content-Type': 'application/json'
                }
                
                logger.info(f"Making {method.upper()} request to {url}")
                if method.lower() == 'get':
                    response = self.session.get(url, headers=headers, params=params)
                else:
                    response = self.session.post(url, headers=headers, json=data)
                
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
        """Get company turnover from VAT returns"""
        logger.info(f"Getting turnover for VAT number {vat_number}")
        
        try:
            # Get VAT returns for the last 4 quarters
            url = f"{self.base_url}/organisations/vat/{vat_number}/returns"
            current_time = datetime.fromisoformat("2025-01-07T20:10:23+08:00")
            
            params = {
                'from': (current_time - timedelta(days=365)).strftime('%Y-%m-%d'),
                'to': current_time.strftime('%Y-%m-%d')
            }
            
            response = self.make_request(url, params=params)
            if not response or 'returns' not in response:
                logger.warning(f"No VAT returns found for {vat_number}")
                return None
                
            # Calculate annual turnover from VAT returns
            total_turnover = 0
            returns = response.get('returns', [])
            
            if not returns:
                logger.warning(f"No VAT returns data available for {vat_number}")
                return None
                
            # Sum up the net VAT due from each return
            # Box 6 contains the total value of sales and all other outputs
            valid_returns = 0
            for vat_return in returns:
                if 'periodKey' in vat_return and 'box6' in vat_return:
                    total_turnover += float(vat_return['box6'])
                    valid_returns += 1
            
            if valid_returns == 0:
                logger.warning(f"No valid VAT returns found for {vat_number}")
                return None
            
            # Calculate average quarterly turnover and extrapolate to annual
            if valid_returns < 4:
                annual_turnover = (total_turnover / valid_returns) * 4
            else:
                annual_turnover = total_turnover
            
            return annual_turnover
            
        except Exception as e:
            logger.error(f"Error getting turnover for {vat_number}: {str(e)}")
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
