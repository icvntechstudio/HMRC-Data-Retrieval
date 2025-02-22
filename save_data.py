import csv
import logging

logger = logging.getLogger(__name__)

def save_company_data(data, file_path):
    if not data:
        logger.debug("No data to save.")
        return

    fieldnames = data[0].keys()
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    logger.debug(f"Data saved to {file_path}")

# Example usage
if __name__ == "__main__":
    data = [
        {
            "Company Status": "Active",
            "Company Registration Number": "12345678",
            "Company Name": "Example Ltd",
            "Address1": "123 Example Street",
            "Address2": "",
            "Town": "Example Town",
            "County": "Example County",
            "Postcode": "EX1 2MP",
            "State": "Example State",
            "Turnover Band": "1M-5M",
            "Phone": "0123456789",
            "Title": "Mr",
            "Forename": "John",
            "Surname": "Doe",
            "Position": "CEO",
            "Age": "45",
            "Contact Email": "john.doe@example.com",
            "SIC Code": "1234",
            "Year Started": "2000",
            "URN": "URN123456",
            "VAT #": "VAT123456"
        }
        # Add more rows as needed
    ]

    save_company_data(data, '/home/highcenburg/DEV/Clients/Data Retrieval/company_data.csv')
