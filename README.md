# Companies House Data Retrieval System

A Python-based system for retrieving and filtering company data from Companies House API, focusing on companies with specific turnover ranges and director age criteria.

## Features

- Retrieves company data from Companies House API
- Filters companies based on:
  - Turnover range (£1M - £1B)
  - Director age (at least one director aged 50+)
  - SIC codes (cleaning and waste management services)
- Processes both Companies House and HMRC turnover data
- Exports filtered results to CSV with detailed company information

## Prerequisites

- Python 3.12+
- Companies House API key
- Required Python packages (see requirements.txt)

## Installation

1. Clone the repository
2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install required packages:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file in the project root with your Companies House API key:
```
COMPANIES_API_KEY=your_api_key_here
```

## Usage

Run the main script:
```bash
python main.py
```

The script will:
1. Search for companies in specified SIC codes
2. Filter companies based on turnover and director age criteria
3. Save results to a CSV file in the `data` directory

## Output

The script generates a CSV file with the following information for each company:
- Company number and name
- Company status and type
- Incorporation date
- SIC codes
- Registered office address
- Directors over 50 (with ages)
- Companies House turnover
- HMRC turnover
- Last accounts date
- Business category
- VAT number

## File Structure

- `main.py`: Main script for data retrieval and processing
- `hmrc_client.py`: HMRC API client simulation
- `requirements.txt`: Required Python packages
- `.env`: Environment variables (API keys)
- `data/`: Directory for output CSV files

## SIC Codes Covered

### Cleaning Services
- 81210: General cleaning of buildings
- 81220: Other building and industrial cleaning
- 81290: Other cleaning activities
- 81200: General cleaning activities
- 81300: Landscaping activities
- 82990: Other business support activities

### Waste Management
- 38110: Collection of non-hazardous waste
- 38320: Recovery of sorted materials
- 38220: Treatment and disposal of hazardous waste
- 38210: Treatment and disposal of non-hazardous waste

## Error Handling

The system includes comprehensive error handling and logging:
- API rate limiting
- Connection timeouts
- Data parsing errors
- Missing or invalid data

## Logging

Detailed logs are provided showing:
- Companies being processed
- Turnover values found
- Director information
- Filtering decisions
- Any errors or issues encountered

## Notes

- The HMRC client currently uses simulated data
- API rate limiting is implemented to respect Companies House limits
- The system is configured to handle various turnover data formats
- Director age calculation uses the first day of birth month when only month and year are available