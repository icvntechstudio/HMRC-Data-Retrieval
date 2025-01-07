from hmrc_client import HMRCClient
import logging

logging.basicConfig(level=logging.INFO)

def test_authentication():
    client = HMRCClient()
    try:
        access_token = client.authenticate()
        if access_token:
            print(f"Authentication successful!")
            print(f"Access token: {access_token[:10]}...")  # Only show first 10 chars for security
        else:
            print("Failed to get access token")
    except Exception as e:
        print(f"Authentication failed: {str(e)}")

if __name__ == "__main__":
    test_authentication()
