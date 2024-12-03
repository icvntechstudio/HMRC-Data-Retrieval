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
    df = pd.read_excel("./data/Business Acquisition HSM Maintenance and AV ATP 2024.xlsx", sheet_name='3rd_Order29052401')
except FileNotFoundError:
    print("Error: File not found. Please ensure the file path is correct.")
    exit()
except Exception as e:
    print(f"An error occurred while reading the file: {e}")
    exit()

# Print columns and sample data for inspection
print("Columns in the DataFrame:", df.columns)
print(df.head())  # View the first few rows to identify relevant data

# Print unique turnover bands to check the format
print("\nUnique turnover bands in the data:")
print(df['Turnover Banding'].unique())

# Function to convert 'Turnover Banding' to numeric values
def convert_turnover(turnover_str):
    if pd.isna(turnover_str):
        return None
    
    # Convert to string to handle any numeric values
    turnover_str = str(turnover_str).strip()
    
    # All bands in this sheet are £1M+ except for bands A-I
    # Return the original band if it's £1M or more
    if turnover_str.startswith(('J:', 'K:', 'L:', 'M:', 'N:', 'O:', 'P:', 'Q:', 'R:', 'S:', 'T:', 'U:', 'V:')):
        return turnover_str
    return None

# Convert 'Turnover Banding' to numeric values
df['Turnover Banding'] = df['Turnover Banding'].apply(convert_turnover)

# Print the results after conversion
print("\nTurnover bands after conversion:")
print(df['Turnover Banding'].value_counts())

# Process each row
for index, row in df.iterrows():
    company_name = row['Company Name']
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

# Filter for companies with £1M+ turnover
filtered_df = df[df['Turnover Banding'].notna()]

# Print the number of companies after filtering
print(f"\nNumber of companies after filtering: {len(filtered_df)}")

# Display sample of filtered data
print("\nSample of filtered companies:")
print(filtered_df[['Company Name', 'Turnover Banding']].head())

# Save filtered data to a CSV file
filtered_df.to_csv("./data/filtered_companies_3rd_sheet.csv", index=False)
print("\nData saved to 'filtered_companies_3rd_sheet.csv'")
