import os
import pandas as pd
import requests
import re
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get API key from environment variable
api_key = os.getenv('COMPANIES_API_KEY')
if not api_key:
    raise ValueError("COMPANIES_API_KEY environment variable is not set")

def get_company_info(company_number):
    url = f"https://api.companieshouse.gov.uk/company/{company_number}"
    headers = {"Authorization": f"Bearer {api_key}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def get_directors(company_number):
    company_info = get_company_info(company_number)
    return company_info.get('officers', [])  # Handle missing 'officers' key

def extract_company_number(company_name):
    match = re.search(r'\((\d{8})\)', company_name)  # Adjust pattern as needed
    return match.group(1) if match else None

# Load the Excel sheet
try:
    df = pd.read_excel("./data/Business Acquisition HSM Maintenance and AV ATP 2024.xlsx", sheet_name='5th_130924(10446)')
except FileNotFoundError:
    print("Error: File not found. Please ensure the file path is correct.")
    exit()
except Exception as e:
    print(f"An error occurred while reading the file: {e}")
    exit()

# Print columns and sample data for inspection
print("Columns in the DataFrame:", df.columns)
print(df.head())  # View the first few rows to identify relevant data

# Function to convert 'Turnover Banding' to numeric values
def convert_turnover(turnover_str):
    if pd.isna(turnover_str):
        return None
    # Only return values for bands F and above (£1M+)
    if turnover_str.startswith(('F:', 'G:', 'H:', 'I:')):
        return turnover_str
    return None

# Convert TurnoverBand to numeric values
df['TurnoverBand'] = df['TurnoverBand'].apply(convert_turnover)

# Check the turnover values to ensure conversion worked
print("Turnover values after conversion:", df['TurnoverBand'].head())

# Process each row
for index, row in df.iterrows():
    company_name = row['CompanyName']
    company_number = extract_company_number(company_name)
    
    if company_number:
        try:
            directors = get_directors(company_number)
            filtered_directors = [director for director in directors if director.get('age', 0) >= 50]
            df.at[index, 'Filtered Directors'] = str(filtered_directors)
        except Exception as e:
            print(f"Error fetching data for company {company_name}: {e}")
            df.at[index, 'Filtered Directors'] = 'Error retrieving directors'
    else:
        df.at[index, 'Filtered Directors'] = 'No company number found'

# Check the 'Filtered Directors' values
print("Filtered Directors values:", df['Filtered Directors'].head())

# Filter for companies with £1M+ turnover (bands F, G, H, I)
filtered_df = df[df['TurnoverBand'].notna()]

# Display the filtered DataFrame
print(filtered_df)

# Save filtered data to a CSV file
filtered_df.to_csv("./data/filtered_companies_5th_sheet.csv", index=False)
print("Data saved to 'filtered_companies_5th_sheet.csv'")
