import os
import requests
from dotenv import load_dotenv
import pandas as pd
from typing import Dict, Any, List
import logging
from datetime import datetime
import time

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CompanyDataRetriever:
    def __init__(self):
        self.companies_house_api_key = os.getenv('COMPANIES_API_KEY')
        self.base_url = 'https://api.company-information.service.gov.uk'
        
    def search_company(self, company_name: str) -> List[Dict[str, Any]]:
        """Search for a company by name"""
        endpoint = f'{self.base_url}/search/companies'
        params = {
            'q': company_name,
            'items_per_page': 5  # Limit results to avoid too many API calls
        }
        
        response = requests.get(
            endpoint,
            auth=(self.companies_house_api_key, ''),
            params=params
        )
        
        if response.status_code == 200:
            data = response.json()
            return data.get('items', [])
        else:
            logger.error(f"Failed to search company {company_name}: {response.status_code}")
            return []

    def get_company_basic_info(self, company_number: str) -> Dict[str, Any]:
        """Get basic company information from Companies House"""
        endpoint = f'{self.base_url}/company/{company_number}'
        response = requests.get(
            endpoint,
            auth=(self.companies_house_api_key, '')
        )
        
        if response.status_code == 200:
            data = response.json()
            return {
                'company_name': data.get('company_name'),
                'company_status': data.get('company_status'),
                'company_number': data.get('company_number'),
                'incorporation_date': data.get('date_of_creation'),
                'registered_office_address': {
                    'address_line_1': data.get('registered_office_address', {}).get('address_line_1'),
                    'address_line_2': data.get('registered_office_address', {}).get('address_line_2'),
                    'locality': data.get('registered_office_address', {}).get('locality'),
                    'region': data.get('registered_office_address', {}).get('region'),
                    'postal_code': data.get('registered_office_address', {}).get('postal_code'),
                },
                'sic_codes': data.get('sic_codes', [])
            }
        else:
            logger.error(f"Failed to get company info: {response.status_code}")
            return None

    def get_company_accounts(self, company_number: str) -> Dict[str, Any]:
        """Get company accounts information from Companies House"""
        endpoint = f'{self.base_url}/company/{company_number}/filing-history'
        params = {
            'category': 'accounts',
            'items_per_page': 1  # Get most recent
        }
        
        response = requests.get(
            endpoint,
            auth=(self.companies_house_api_key, ''),
            params=params
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get('items'):
                latest_accounts = data['items'][0]
                return {
                    'accounts_date': latest_accounts.get('date'),
                    'accounts_type': latest_accounts.get('type'),
                    'accounts_category': latest_accounts.get('category'),
                    'description': latest_accounts.get('description')
                }
        
        logger.error(f"Failed to get accounts info: {response.status_code}")
        return None

    def process_company_names(self, company_names: List[str]) -> List[Dict[str, Any]]:
        """Process a list of company names and get their data"""
        results = []
        
        for company_name in company_names:
            logger.info(f"Processing company: {company_name}")
            
            # Search for the company
            search_results = self.search_company(company_name)
            
            if search_results:
                # Get the first matching company
                company = search_results[0]
                company_number = company.get('company_number')
                
                if company_number:
                    # Get detailed information
                    basic_info = self.get_company_basic_info(company_number)
                    if basic_info:
                        # Get accounts information
                        accounts_info = self.get_company_accounts(company_number)
                        if accounts_info:
                            basic_info['financial_info'] = accounts_info
                        
                        results.append(basic_info)
                        
            # Add a small delay to avoid hitting rate limits
            time.sleep(0.1)
            
        return results

def main():
    # Read company names from the file
    with open('company_names.txt', 'r') as f:
        company_names = [line.strip() for line in f if line.strip()]
    
    retriever = CompanyDataRetriever()
    results = retriever.process_company_names(company_names)
    
    # Save results to CSV
    if results:
        df = pd.DataFrame(results)
        df.to_csv('company_data.csv', index=False)
        logger.info("Data saved to company_data.csv")
        
        # Print summary
        logger.info(f"Successfully processed {len(results)} companies")

if __name__ == "__main__":
    main()
