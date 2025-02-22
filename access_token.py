import requests
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# HMRC API Credentials (Ensure these are set in your .env file)
CLIENT_ID = os.getenv("HMRC_CLIENT_ID")
CLIENT_SECRET = os.getenv("HMRC_CLIENT_SECRET")

# HMRC API URL (Use production URL if needed)
BASE_URL = "https://api.service.hmrc.gov.uk"
TOKEN_URL = f"{BASE_URL}/oauth/token"

# Ensure credentials are set
if not CLIENT_ID or not CLIENT_SECRET:
    raise ValueError("CLIENT_ID or CLIENT_SECRET is missing. Check your .env file.")

# Request payload
data = {
    "grant_type": "client_credentials",
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "scope": "read:vat"  # Adjust scope if needed
}

# Request headers
headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "application/json"
}

# Make the request
response = requests.post(TOKEN_URL, headers=headers, data=data)

# Handle response
if response.status_code == 200:
    access_token = response.json().get("access_token")
    print("Access Token:", access_token)
else:
    print(f"Error {response.status_code}: {response.text}")
