import pandas as pd
import requests
import re

# Load your data
file_path = '/content/drive/MyDrive/maps_results.xlsx'
df = pd.read_excel(file_path, sheet_name='Sheet1')

def parse_address(address):
    # Initialize variables
    street = ''
    house_number = ''
    orientation_number = ''
    municipality = ''
    zip_code = ''

    # Extract postal code
    zip_match = re.search(r'\b\d{3}\s?\d{2}\b', address)
    if zip_match:
        zip_code = zip_match.group(0)

    # Extract municipality (city) and handle different formats
    municipality_match = re.search(r'\bPraha\b', address)
    if municipality_match:
      municipality = 'Praha'
    else:
      municipality_match = re.search(r'\b\d{3}\s?\d{2}\b[^,]\s*(.*)$', address)
      if municipality_match:
          municipality = municipality_match.group(1).strip()
      else:
          # If no zip code is present, get the part after the last comma
          municipality_match = re.search(r',\s*([^,]+?)(?:,|$)', address)
          if municipality_match:
              municipality = municipality_match.group(1).strip()

    # Extract street, house number, and orientation number
    street_match = re.search(r'([^\d,]+?)\s*(\d+)(?:/(\d+\w?))?', address)
    if street_match:
        street = street_match.group(1).strip()
        house_number = street_match.group(2)
        orientation_number = street_match.group(3) if street_match.group(3) else ''

    return street, house_number, orientation_number, municipality, zip_code

# Apply parsing function to each row
df[['Street', 'Cp', 'Orientation Number', 'Municipality Name', 'Zip']] = df['Address'].apply(
    lambda x: pd.Series(parse_address(x))
)

# Function to get standardized address from RUIAN API
def get_standardized_address(row):
    base_url = "https://ruian.fnx.io/api/v1/ruian/validate"
    api_key = "f14dfd0b162c72ede99889c7d071b448becd245f2edb2d5f2f4e6b8faee1cd2e"  # Replace with your actual API key

    params = {
        "apiKey": api_key,
        "municipalityName": row['Municipality Name'],
        "zip": row['Zip'].replace(' ', ''),
        "cp": row['Cp'],
        "co": row['Orientation Number'],
        "street": row['Street']
    }

    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        data = response.json()
        if data['status'] in ["POSSIBLE", "MATCH"]:
            place = data['place']
            if place['co']:
                standardized_address = f"{place['streetName']} {place['cp']}/{place['co']}, {place['municipalityName']} {place['zip']}"
            else:
                standardized_address = f"{place['streetName']} {place['cp']}, {place['municipalityName']} {place['zip']}"
            return standardized_address
        else:
            return None
    else:
        return None

# Apply the function to each address in the dataframe
df['Standardized Address'] = df.apply(get_standardized_address, axis=1)

# Save the updated dataframe to a new Excel file
output_file_path = '/content/drive/MyDrive/maps_results_ruian.xlsx'
df.to_excel(output_file_path, index=False)

print(f"Standardized addresses saved to {output_file_path}")
