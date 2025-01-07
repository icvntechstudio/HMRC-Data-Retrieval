The code now:

1. Makes a request to the HMRC API endpoint /organisations/vat/{vat_number}/returns
2. Gets VAT returns for the last 365 days
3. Calculates the total turnover by summing Box 6 values (total sales) from each VAT return
4. Extrapolates to annual turnover if needed (e.g., if only 2 quarters of data are available, it multiplies by 2 to get annual estimate)
