# This is a brief explanation of the project.

- file.py: Reads company_names.txt file and processes each line to add quotes and a comma
- main.py: Goes through the list of company names and searches for each one in the Companies House API
- hmrc_client.py: Handles the API request and response

## How to Run the Project

1. **Clone the Repository**: If you haven't already, clone the repository using the following command:
   ```bash
   git clone https://github.com/icvntechstudio/HMRC-Data-Retrieval.git
   ```

2. Navigate to the Project Directory and Change into the project directory:
   ```bash
   cd HMRC-Data-Retrieval
   ```

3. Install Dependencies. Make sure to install all necessary dependencies. If you're using Python, you can do this with:


   ```bash
   python -m venv env
   source env/bin/activate
   python -m pip install -r requirements.txt
   ```

4. Run the Application. Start the application using the following command:

   ```bash
   python -m main
   ```