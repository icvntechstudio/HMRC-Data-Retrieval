import os
import requests
import csv
import logging
from datetime import datetime
from urllib.parse import quote  # Import quote for URL encoding
from base64 import b64encode  # Import b64encode for encoding the API key
from hmrc_client import HMRCClient
from save_data import save_company_data  # Import the save_company_data function

# Configure logging
logging.basicConfig(level=logging.DEBUG)  # Change to DEBUG level
logger = logging.getLogger(__name__)

# Your Companies House API Key
api_key = os.getenv('COMPANIES_API_KEY')
hmrc_api_key = os.getenv('HMRC_CLIENT_ID')

# Base64 encode the API key
encoded_api_key = b64encode(f"{api_key}:".encode()).decode()

# SIC codes for cleaning and waste management companies
CLEANING_WASTE_SIC_CODES = [
    '81210',  # General cleaning of buildings
    '81221',  # Window cleaning services
    '81222',  # Specialized cleaning services
    '81290',  # Other cleaning services
    '38110',  # Collection of non-hazardous waste
    '38120',  # Collection of hazardous waste
    '38210',  # Treatment and disposal of non-hazardous waste
    '38220',  # Treatment and disposal of hazardous waste
    '38310',  # Dismantling of wrecks
    '38320'   # Recovery of sorted materials
]

# Minimum age for directors
MIN_DIRECTOR_AGE = 50
# Minimum turnover in pounds
MIN_TURNOVER = 1000000  # £1M

# Load company names from the processed-9k-leads.txt file
def load_company_names():
    company_names = []
    with open('processed-9k-leads.txt', 'r') as file:
        for line in file:
            # Clean up the company name (remove quotes and whitespace)
            company_name = line.strip().strip('",')
            if company_name:  # Only add non-empty names
                company_names.append(company_name)
    return company_names

# Function to search for company registration number by company name
def search_company_by_name(company_name):
    encoded_company_name = quote(company_name)  # URL-encode the company name
    url = f"https://api.company-information.service.gov.uk/search/companies?q={encoded_company_name}"
    timeout = 10  # Add a timeout of 10 seconds
    headers = {
        'Authorization': f'Basic {encoded_api_key}',
        'Accept': 'application/json'
    }
    logger.debug(f"Searching for company: {company_name}")
    logger.debug(f"Request URL: {url}")
    logger.debug(f"Encoded API Key: {encoded_api_key}")
    try:
        response = requests.get(url, headers=headers, timeout=timeout)  # Add headers
        response.raise_for_status()
        data = response.json()
        if "items" in data and data["items"]:
            company = data["items"][0]
            return company["company_number"]
        else:
            logger.info(f"No company found for: {company_name}")
            return None
    except requests.exceptions.Timeout:
        logger.error(f"Request timed out while searching for company {company_name}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error searching for company {company_name}: {str(e)}")
        return None

# Function to get company details (name, address, SIC codes, status) from Companies House API
def get_company_details(company_number):
    url = f"https://api.company-information.service.gov.uk/company/{company_number}"
    timeout = 10  # Add a timeout of 10 seconds
    headers = {
        'Authorization': f'Basic {encoded_api_key}',
        'Accept': 'application/json'
    }
    try:
        response = requests.get(url, headers=headers, timeout=timeout)  # Add headers
        response.raise_for_status()
        data = response.json()
        # Basic company info
        name = data.get("company_name", "N/A")
        status = data.get("company_status", "N/A")
        
        # Address details
        address = data.get("registered_office_address", {})
        address1 = address.get("address_line_1", "")
        address2 = address.get("address_line_2", "")
        town = address.get("locality", "")
        county = address.get("region", "")
        postcode = address.get("postal_code", "")
        country = address.get("country", "United Kingdom")
        
        # Additional details
        sic_codes = data.get("sic_codes", [])
        is_active = status.lower() == "active"
        incorporation_date = data.get("date_of_creation", "N/A")
        vat_number = f"GB{company_number}"  # Placeholder, actual VAT number needs HMRC API
        urn = f"URN{company_number}"  # Placeholder, actual URN needs different source
        
        # Contact details (these would need to be fetched from a different source)
        phone = "N/A"  # Would need additional API or data source
        email = "N/A"  # Would need additional API or data source
        
        return {
            "name": name,
            "status": status,
            "address1": address1,
            "address2": address2,
            "town": town,
            "county": county,
            "postcode": postcode,
            "country": country,
            "sic_codes": sic_codes,
            "is_active": is_active,
            "incorporation_date": incorporation_date,
            "vat_number": vat_number,
            "urn": urn,
            "phone": phone,
            "email": email
        }
    except requests.exceptions.Timeout:
        logger.error(f"Request timed out while getting details for company {company_number}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting details for company {company_number}: {str(e)}")
        return None

# Function to get all directors' details (name and age) from Companies House API
def get_directors_details(company_number):
    url = f"https://api.company-information.service.gov.uk/company/{company_number}/officers"
    headers = {
        'Authorization': f'Basic {encoded_api_key}',
        'Accept': 'application/json'
    }
    response = requests.get(url, headers=headers)
    directors = []
    
    if response.status_code == 200:
        try:
            data = response.json()
            if "items" in data:
                for officer in data["items"]:
                    if officer.get("officer_role") == "director" and officer.get("resigned_on") is None:  # Only active directors
                        # Extract name components
                        full_name = officer.get("name", "")
                        name_parts = full_name.split(", ")
                        if len(name_parts) == 2:
                            surname, forenames = name_parts
                            # Try to extract title and forename
                            forename_parts = forenames.split()
                            title = forename_parts[0] if len(forename_parts) > 1 and forename_parts[0] in ["Mr", "Mrs", "Ms", "Dr", "Prof"] else ""
                            forename = " ".join(forename_parts[1:] if title else forename_parts)
                        else:
                            surname = full_name
                            forename = ""
                            title = ""
                        
                        # Calculate age
                        birth_year = officer.get("date_of_birth", {}).get("year")
                        if birth_year:
                            age = datetime.now().year - int(birth_year)
                            if age >= MIN_DIRECTOR_AGE:
                                director_info = {
                                    "title": title,
                                    "forename": forename,
                                    "surname": surname,
                                    "position": "Active Director",
                                    "age": age
                                }
                                directors.append(director_info)
        except requests.exceptions.JSONDecodeError:
            logger.error(f"Invalid JSON response for officers. Response: {response.text}")
    return directors

# Function to get company turnover using HMRCClient
def get_company_turnover(client, vat_number):
    try:
        return client.get_company_turnover(vat_number)
    except Exception as e:
        logger.error(f"Error fetching turnover for VAT number {vat_number}: {e}")
        return None

# Initialize the HMRC client
hmrc_client = HMRCClient()

def process_companies():
    # Define the fieldnames for our CSV output
    fieldnames = [
        'Company Status',
        'Company Registration Number',
        'Company Name',
        'Address1',
        'Address2',
        'Town',
        'County',
        'Postcode',
        'State',
        'Turnover Band',
        'Phone',
        'Title',
        'Forename',
        'Surname',
        'Position',
        'Age',
        'Contact Email',
        'SIC Code',
        'Year Started',
        'URN',
        'VAT #'
    ]
    
    # List to hold all company data
    all_company_data = []
    
    # Process each company
    processed_count = 0
    for name in load_company_names():
        company_number = search_company_by_name(name)
        if company_number:
            company_details = get_company_details(company_number)
            if not company_details or not company_details["is_active"]:
                logger.info(f"Skipping company {name} ({company_number}) as it is not active.")
                continue
            
            # Check if company is in cleaning/waste management sector
            if not any(code in CLEANING_WASTE_SIC_CODES for code in company_details["sic_codes"]):
                logger.info(f"Skipping company {name} ({company_number}) - not in cleaning/waste management sector.")
                continue
            
            directors = get_directors_details(company_number)
            vat_number = f"GB{company_number}"  # Ensure VAT number format is correct
            turnover = get_company_turnover(hmrc_client, vat_number)
            
            if turnover and turnover >= MIN_TURNOVER:
                # Filter directors by age
                qualifying_directors = [director for director in directors if director["age"] >= MIN_DIRECTOR_AGE]
                if qualifying_directors:
                    # Add a row for each qualifying director
                    for director in qualifying_directors:
                        all_company_data.append({
                            'Company Status': company_details["status"],
                            'Company Registration Number': company_number,
                            'Company Name': company_details["name"],
                            'Address1': company_details["address1"],
                            'Address2': company_details["address2"],
                            'Town': company_details["town"],
                            'County': company_details["county"],
                            'Postcode': company_details["postcode"],
                            'State': company_details["country"],
                            'Turnover Band': "£1M and above" if turnover >= MIN_TURNOVER else "Below £1M",
                            'Phone': company_details["phone"],
                            'Title': director["title"],
                            'Forename': director["forename"],
                            'Surname': director["surname"],
                            'Position': director["position"],
                            'Age': director["age"],
                            'Contact Email': company_details["email"],
                            'SIC Code': ", ".join(company_details["sic_codes"]),
                            'Year Started': company_details["incorporation_date"].split("-")[0] if company_details["incorporation_date"] != "N/A" else "N/A",
                            'URN': company_details["urn"],
                            'VAT #': company_details["vat_number"]
                        })
                    logger.info(f"Company {company_details['name']} processed successfully.")
                    processed_count += 1
                else:
                    logger.info(f"Skipping company {name} ({company_number}) - no directors aged {MIN_DIRECTOR_AGE} or older.")
            else:
                logger.info(f"Skipping company {name} ({company_number}) - turnover below £1M.")
        else:
            logger.info(f"Company {name} not found.")
    
    # Save all company data to CSV
    logger.debug(f"Saving {len(all_company_data)} companies to CSV.")
    save_company_data(all_company_data, '/home/highcenburg/DEV/Clients/Data Retrieval/company_data.csv')

if __name__ == "__main__":
    process_companies()
