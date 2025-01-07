import requests
from datetime import datetime

# Your Companies House API Key
api_key = 'YOUR_API_KEY'

# Function to get director age from Companies House API
def get_director_age(company_number):
    url = f"https://api.company-information.service.gov.uk/company/{company_number}/officers"
    response = requests.get(url, auth=(api_key, ''))
    data = response.json()
    
    # Check if there are officers
    if "items" in data:
        for officer in data["items"]:
            # Only check for active directors
            if officer["officer_role"] == "director":
                birth_year = int(officer["date_of_birth"]["year"])
                age = datetime.now().year - birth_year
                return age
    return None

# Example use
company_numbers = ["11496595", "12475415"]  # Add more company numbers
for number in company_numbers:
    age = get_director_age(number)
    if age:
        print(f"Company {number}: Director is {'over 50' if age > 50 else 'under 50'} (Age: {age})")
    else:
        print(f"Company {number}: No director age available.")