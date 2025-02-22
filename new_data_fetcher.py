import requests
import csv
from datetime import datetime
import time

# Constants
API_KEY = "fbee01fe-a663-421e-96ee-2c952f569983"
BASE_URL = "https://api.company-information.service.gov.uk"

def search_company(company_name):
    """Search for a company and return its number"""
    url = f"{BASE_URL}/search/companies"
    headers = {"Authorization": API_KEY}
    params = {
        "q": company_name,
        "items_per_page": 1
    }
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            if data.get("items") and len(data["items"]) > 0:
                return data["items"][0].get("company_number")
    except Exception as e:
        print(f"Error searching for company {company_name}: {str(e)}")
    return None

def get_company_profile(company_number):
    """Get company profile with error handling"""
    url = f"{BASE_URL}/company/{company_number}"
    headers = {"Authorization": API_KEY}
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            print(f"Company not found: {company_number}")
        else:
            print(f"Error fetching company profile: {response.status_code}")
    except Exception as e:
        print(f"Exception fetching company profile: {str(e)}")
    return None

def get_company_officers(company_number):
    """Get company officers with error handling"""
    url = f"{BASE_URL}/company/{company_number}/officers"
    headers = {"Authorization": API_KEY}
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json().get("items", [])
        elif response.status_code == 404:
            print(f"Officers not found for company: {company_number}")
        else:
            print(f"Error fetching officers: {response.status_code}")
    except Exception as e:
        print(f"Exception fetching officers: {str(e)}")
    return []

def get_filing_history(company_number):
    """Get filing history with error handling"""
    url = f"{BASE_URL}/company/{company_number}/filing-history"
    headers = {"Authorization": API_KEY}
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json().get("items", [])
        elif response.status_code == 404:
            print(f"Filing history not found for company: {company_number}")
        else:
            print(f"Error fetching filing history: {response.status_code}")
    except Exception as e:
        print(f"Exception fetching filing history: {str(e)}")
    return []

def calculate_age(date_of_birth):
    today = datetime.today()
    return today.year - date_of_birth.year - ((today.month, today.day) < (date_of_birth.month, date_of_birth.day))

def gather_company_data(company_names):
    data = []
    for company_name in company_names:
        print(f"Processing: {company_name}")
        
        # Search for company number
        company_number = search_company(company_name)
        if not company_number:
            print(f"Could not find company number for: {company_name}")
            continue

        # Add delay to avoid rate limiting
        time.sleep(0.1)

        # Fetch profile, officers, and filings
        profile = get_company_profile(company_number)
        if not profile:
            continue

        officers = get_company_officers(company_number)
        filings = get_filing_history(company_number)

        try:
            company_data = {
                "Company Name": profile.get("company_name", ""),
                "Company Status": profile.get("company_status", ""),
                "Company Registration Number": company_number,
                "Address1": profile.get("registered_office_address", {}).get("address_line_1", ""),
                "Address2": profile.get("registered_office_address", {}).get("address_line_2", ""),
                "Town": profile.get("registered_office_address", {}).get("locality", ""),
                "County": profile.get("registered_office_address", {}).get("region", ""),
                "Postcode": profile.get("registered_office_address", {}).get("postal_code", ""),
                "State": "United Kingdom",
                "SIC Code": ", ".join(profile.get("sic_codes", [])),
                "Year Started": profile.get("date_of_creation", ""),
                "Turnover Band": "",  # Will be updated if found in filings
                "Directors (Aged > 50)": ""  # Will be updated if found in officers
            }

            # Extract directors over 50
            directors_over_50 = []
            for officer in officers:
                if officer.get("officer_role") == "director" and not officer.get("resigned_on"):
                    dob = officer.get("date_of_birth")
                    if dob:
                        try:
                            age = calculate_age(datetime(dob["year"], dob["month"], 1))
                            if age >= 50:
                                director_info = f"{officer.get('name', '')} (Age: {age})"
                                directors_over_50.append(director_info)
                        except Exception as e:
                            print(f"Error calculating age for director: {str(e)}")

            company_data["Directors (Aged > 50)"] = "; ".join(directors_over_50)

            # Extract financial turnover from filings
            for filing in filings:
                if filing.get("category") == "accounts":
                    company_data["Turnover Band"] = "£1M and above"  # Placeholder
                    break

            data.append(company_data)
            print(f"Successfully processed: {company_name}")

        except Exception as e:
            print(f"Error processing company data for {company_name}: {str(e)}")
            continue

        # Add delay between companies
        time.sleep(0.1)

    return data

def save_to_csv(data):
    if not data:
        print("No data to save")
        return

    fieldnames = [
        "Company Name", "Company Status", "Company Registration Number",
        "Address1", "Address2", "Town", "County", "Postcode", "State",
        "SIC Code", "Year Started", "Directors (Aged > 50)", "Turnover Band"
    ]

    try:
        with open("company_data2.csv", "w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        print(f"Successfully saved {len(data)} companies to company_data.csv")
    except Exception as e:
        print(f"Error saving to CSV: {str(e)}")

def main():
    # Read company names from file
    try:
        with open("new-leads.txt", "r", encoding="utf-8") as file:
            company_names = [line.strip() for line in file if line.strip()]
    except Exception as e:
        print(f"Error reading company names file: {str(e)}")
        return

    print(f"Processing {len(company_names)} companies...")
    data = gather_company_data(company_names)
    save_to_csv(data)

if __name__ == "__main__":
    main()