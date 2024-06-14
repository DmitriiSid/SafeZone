import pandas as pd
import requests
import re
import unicodedata
from tqdm import tqdm
import concurrent.futures

# Load the Excel file
def load_data(file_path):
    return pd.read_csv(file_path)

def geocode(query):
    API_KEY = 'FazBooX7wHyVDYN7xmuEAcPa2tTuOE1h8H-n0abHv8A'
    url = 'https://api.mapy.cz/v1/geocode'

    params = {
        'apikey': API_KEY,
        'query': query,
        'lang': 'cs',
        'limit': '15'
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        # Vrátí seznam adres
        return [{
            'name': item['name'],
            'label': item['label'],
            'location': item['location'],
            'regionalStructure': item.get('regionalStructure', [])
        } for item in data.get('items', [])]
    except requests.RequestException as e:
        print(f'HTTP Request failed: {e}')
    except ValueError:
        print('Error decoding JSON')

def normalize_text(text):
    if not isinstance(text, str):
        return ''
    # Odstranit interpunkci, převést na malá písmena, odstranit mezery a diakritiku
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')
    text = re.sub(r'\s+', ' ', re.sub(r'[^\w\s]', '', text.strip().lower()))
    return text

def extract_street(address):
    if not isinstance(address, str):
        return ''
    # Odstranit "č.p." a extrahovat název ulice z adresy (první část před čárkou, odstranit číslo popisné)
    address = re.sub(r'\bč\.?p\.?\b', '', address, flags=re.IGNORECASE)
    street = re.split(r'\d+', address.split(',')[0])[0].strip()
    return normalize_text(street)

def google_maps_api(name, address, api_key, original_address):
    # Použití Mapy.cz k ověření adresy
    mapy_cz_results = geocode(name)

    # Extrahování a normalizace ulice z původní adresy
    normalized_original_street = extract_street(original_address)

    # Normalizace ulic z Mapy.cz výsledků
    normalized_mapy_cz_streets = [
        normalize_text(regional['name']) for item in mapy_cz_results
        for regional in item.get('regionalStructure', [])
        if regional['type'] == 'regional.street'
    ]

    matching_address = normalized_original_street in normalized_mapy_cz_streets

    if matching_address:
        search_query = f"{name} {address}"
    else:
        search_query = f"{name}"

    # Construct the request URL for the Google Places API - Find Place
    find_place_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    find_params = {
        'input': search_query,
        'inputtype': 'textquery',
        'fields': 'place_id',
        'key': api_key
    }

    find_response = requests.get(find_place_url, params=find_params)
    find_result = find_response.json()

    if find_result['status'] == 'OK':
        if 'candidates' in find_result and len(find_result['candidates']) > 0:
            place_id = find_result['candidates'][0]['place_id']

            # Construct the request URL for the Place Details API
            details_url = "https://maps.googleapis.com/maps/api/place/details/json"
            details_params = {
                'place_id': place_id,
                'fields': 'formatted_address,name,formatted_phone_number,website,opening_hours',
                'key': api_key
            }

            details_response = requests.get(details_url, params=details_params)
            details_result = details_response.json()

            if details_result['status'] == 'OK':
                if 'result' in details_result:
                    details = details_result['result']
                    phone = details.get('formatted_phone_number', 'No phone number available')
                    return {
                        'normalized_address': details.get('formatted_address', ''),
                        'phone': phone,
                        'name': details.get('name', ''),
                        'website': details.get('website', ''),
                        'opening_hours': details.get('opening_hours', '')
                    }

    return {
        'normalized_address': 'Not found',
        'phone': 'No phone number available'
    }

# Compare the data from API with the Excel data
def compare_data(excel_data, api_data):
    results = []
    for index, row in excel_data.iterrows():
        # Převést telefonní čísla na řetězec a rozdělit podle čárky, odstranit mezery kolem čísel
        phone_numbers = [phone.strip() for phone in str(row['Telefon']).split(',')]
        # Zkontrolovat, zda některé číslo obsahuje číslo z api_data['phone']
        phone_match = api_data.get('phone', '') in phone_numbers

        results.append({
            'Name': row['Nazev'],
            'API_Name': api_data.get('name', ''),
            'Phone': row['Telefon'],
            'Phone Match': phone_match,
            'API_Phone': api_data.get('phone', 'No phone number available'),
            'Address': row['Adresa'],
            'API_Address': api_data.get('normalized_address', ''),
            'Email': row['E_mail'],
            'Web': row['Webova_stranka'],
            'API_Web': api_data.get('website', ''),
            'API_Opening Hours': api_data.get('opening_hours', '')
        })
    return pd.DataFrame(results)

# Main function to orchestrate the processing
def main(file_path, api_key):
    data = load_data(file_path)
    data = data[~data["Webova_stranka"].isna()]
    print("Data are loaded")
    all_results = []

    # Define a helper function for processing each row
    def process_row(index, row):
        google_response = google_maps_api(row['Nazev'], row['Adresa'], api_key, row['Adresa'])
        return compare_data(pd.DataFrame([row]), google_response)

    # Use ThreadPoolExecutor to process rows concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers = 8) as executor:
        futures = {executor.submit(process_row, index, row): index for index, row in data.iterrows()}
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
            all_results.append(future.result())

    # Concatenate all results into a single DataFrame
    final_results = pd.concat(all_results)
    return final_results

# Usage example (this call should be outside this script in real use)
API_KEY = 'AIzaSyAnyvXFL8O4kBFnT0rUZ1xhKFpOHrfEuLY'
FILE_PATH = 'in/tables/db_pomoci.csv'
results = main(FILE_PATH, API_KEY)
results.to_csv("out/tables/maps_scraped.csv", index=False)
