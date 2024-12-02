import pandas as pd
import requests
import re

# Replace 'your_api_key' with your actual API key
api_key = 'api_key'

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
    df = pd.read_excel("/content/drive/MyDrive/Business Acquisition HSM Maintenance and AV ATP 2024.xlsx", sheet_name='3rd_Order29052401')
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
    match = re.search(r'£(\d+(?:,\d+)?)', turnover_str)  # Capture numbers with commas
    if match:
        return float(match.group(1).replace(',', ''))  # Remove commas and convert to float
    return None  # Return None if the value can't be converted

# Convert 'Turnover Banding' to numeric
df['Turnover Banding'] = df['Turnover Banding'].apply(convert_turnover)

# Check the turnover values to ensure conversion worked
print("Turnover Banding values after conversion:", df['Turnover Banding'].head())

# Process each row
for index, row in df.iterrows():
    company_name = row['Company Name']
    company_number = extract_company_number(company_name)
    
    if company_number:
        try:
            directors = get_directors(company_number)
            # Filter directors based on age (ensure 'age' field exists)
            filtered_directors = [director for director in directors if director.get('age', 0) >= 50]
            df.at[index, 'Filtered Directors'] = str(filtered_directors)  # Convert list to string
        except Exception as e:
            print(f"Error fetching data for company {company_name}: {e}")
            df.at[index, 'Filtered Directors'] = 'Error retrieving directors'
    else:
        df.at[index, 'Filtered Directors'] = 'No company number found'

# Check the 'Filtered Directors' values
print("Filtered Directors values:", df['Filtered Directors'].head())

# Relax filtering to check the data
filtered_df = df[(df['Turnover Banding'].notnull())]

# Display the filtered DataFrame (without strict conditions)
print(filtered_df)

# Save filtered data to a CSV file
filtered_df.to_csv("/content/filtered_companies.csv", index=False)
print("Data saved to 'filtered_companies.csv'")
