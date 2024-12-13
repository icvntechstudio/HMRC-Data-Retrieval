# Read the input file and process each line
"""
    How to use:
    1. Copy company names from the excel file to a text file.
    2. Ensure the text file is in the same directory as this script.
    3. Run the script using the command: python file.py
"""
input_file = "company_names.txt"
output_file = "processed-9k-leads.txt"

with open(input_file, "r") as file:
    lines = file.readlines()

# Add quotes and a comma to each line
processed_lines = [f'"{line.strip()}",\n' for line in lines]

# Write the processed lines to the output file
with open(output_file, "w") as file:
    file.writelines(processed_lines)

print(f"Processed {len(lines)} lines and saved to {output_file}.")
