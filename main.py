import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import re
from tqdm import tqdm 
import phonenumbers
import phonenumbers.geocoder
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import datetime
import os

RUN_EVIRONMENT = "kagle"

if RUN_EVIRONMENT == "local":
    DB_POMOCI_PATH ="db_pomoci.csv"
    MAX_WORKERS = 2
    SAVE_FILES_PATH = "data/"
    MAPS_SCRAPED = "../data/maps_results.csv"
    FLAGGED_DATA_PATH = f'../data/{datetime.datetime.now().day}_{datetime.datetime.now().strftime("%m")}/db_pomoci_flagged.csv'
    SCRAPED_DATA_PATH = f'../data/{datetime.datetime.now().day}_{datetime.datetime.now().strftime("%m")}/scraped_data.csv'
    if not os.path.exists("../data"):
        os.makedirs("../data")
    if not os.path.exists(f'../data/{datetime.datetime.now().day}_{datetime.datetime.now().strftime("%m")}'):
        os.makedirs(f'../data/{datetime.datetime.now().day}_{datetime.datetime.now().strftime("%m")}')
    pass
elif RUN_EVIRONMENT == "keboola":
    pass
elif RUN_EVIRONMENT == "colab":
    pass
elif RUN_EVIRONMENT == "kagle":
    DB_POMOCI_PATH ="/kaggle/input/db-pomoci/db_pomoci.csv"
    MAX_WORKERS = 12
    SAVE_FILES_PATH = "data/"
    MAPS_SCRAPED = "/kaggle/input/maps-result/maps_results.csv"
    FLAGGED_DATA_PATH = f'/kaggle/working/data/{datetime.datetime.now().day}_{datetime.datetime.now().strftime("%m")}/db_pomoci_flagged.csv'
    SCRAPED_DATA_PATH = '/kaggle/working/data/'
    if not os.path.exists("../data"):
        os.makedirs("../data")
    if not os.path.exists(f'/kaggle/working/data/{datetime.datetime.now().day}_{datetime.datetime.now().strftime("%m")}'):
        os.makedirs(f'/kaggle/working/data/{datetime.datetime.now().day}_{datetime.datetime.now().strftime("%m")}')
else:
    raise EnvironmentError("this environment is not supported")




pd.set_option('display.max_rows', 500)
def ensure_url_format(url):
    if not (url.startswith('www') or url.startswith('http') or url.startswith('https')):
        return 'www.' + url
    return url



def data_prep(subset:bool = False) -> pd.DataFrame:
    db_pomoci = pd.read_csv(DB_POMOCI_PATH)
    df = db_pomoci.rename(columns={"Webová stránka" : "web", "Název":"nazev","E-mail":"email"})
    black_list = ['http://www.dc-brno.cz','www.freeklub.cz', 'http://www.cszs.cz/','http://www.fokustabor.cz/centrum-dusevniho-zdravi-_-komunitni-tym-tabor']
    df = df[(~df["web"].isna()) & (~df['web'].isin(black_list))]
    df['web'] = df['web'].str.replace(r'\s+', '', regex=True)
    df_replace = df.copy()
    df_replace.loc[df_replace['web'].str.contains(r'\.cz/.+'), 'web'] = df_replace['web'].str.replace(r'\.cz/.+', '.cz', regex=True)
    #df.loc[df['web'].str.contains(r'\.cz/.+'), 'web'] = df['web'].str.replace(r'\.cz/.+', '.cz', regex=True)
    # Apply the function to the 'web' column
    df['web'] = df['web'].apply(ensure_url_format)
    df.loc[df['web'].str.startswith('www'), 'web'] = df['web'].str.replace('^www', 'https://www', regex=True)
    df = pd.concat([df, df_replace]).drop_duplicates().reset_index(drop=True)
    if subset:
        df = df.head(100) # ubset first 300 rows
    return df


def scrape_urls(df):    
    email_regex = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
    # Phone regex V2
    phone_regex = re.compile(r"""
        (?:
            \+420[-\s]?        # Optional country code +420 followed by an optional space or dash
        )?
        (?:
            \d{3}[-\s]?        # First part of the phone number (3 digits) followed by an optional space or dash
            \d{3}[-\s]?        # Second part of the phone number (3 digits) followed by an optional space or dash
            \d{3}              # Third part of the phone number (3 digits)
            |                  # OR
            \d{3}[-\s]?        # First part of the phone number (3 digits) followed by an optional space or dash
            \d{2}[-\s]?        # Second part of the phone number (2 digits) followed by an optional space or dash
            \d{2}[-\s]?        # Third part of the phone number (2 digits) followed by an optional space or dash
            \d{2}              # Fourth part of the phone number (2 digits)
        )
    """, re.VERBOSE)

    # List to collect data
    data = []

    # Function to scrape a single URL
    def scrape_website_main_page(url, retries=3, backoff_factor=0.3, page_type='main'):
        for attempt in range(retries):
            try:
                #print(url)
                response = requests.get(url, timeout=10)
                response.raise_for_status()  # Raises an HTTPError if the response status code is 4XX or 5XX
                soup = BeautifulSoup(response.text, 'html.parser')
                emails = set(email_regex.findall(soup.text))
                phones = set(phone_regex.findall(soup.text))
                return emails, phones, url, page_type
            except requests.RequestException as e:
                if attempt < retries - 1:
                    time.sleep(backoff_factor * (2 ** attempt))
                else:
                    return set(), set(), url, page_type

    # Function to scrape potential contact pages
    def scrape_website_contacts(base_url, visited_urls=set(), retries=3, backoff_factor=0.3):
        contact_pages = set()
        emails = set()
        phones = set()

        def get_contact_links(soup, base_url):
            links = []
            for link in soup.find_all('a', href=True):
                if 'kontakt' in link.text.lower() or 'contact' in link.text.lower() or 'kontakty' in link.text.lower() or 'o nás' in link.text.lower()  or 'kdo-jsem' in link.text.lower() or 'o-nas' in link.text.lower():
                    contact_href = urljoin(base_url, link['href'])
                    if contact_href not in visited_urls:
                        links.append(contact_href)
            return links

        def scrape_page(url, page_type):
            if url in visited_urls:
                return set(), set(), url, page_type
            visited_urls.add(url)

            for attempt in range(retries):
                try:
                    response = requests.get(url, timeout=10)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, 'html.parser')

                    page_emails = set(email_regex.findall(soup.text))
                    page_phones = set(phone_regex.findall(soup.text))

                    return page_emails, page_phones, url, page_type
                except requests.RequestException as e:
                    if attempt < retries - 1:
                        time.sleep(backoff_factor * (2 ** attempt))
                    else:
                        return set(), set(), url, page_type

        main_emails, main_phones, main_url, main_page_type = scrape_page(base_url, 'main')
        emails.update(main_emails)
        phones.update(main_phones)
        contact_pages.update(get_contact_links(BeautifulSoup(requests.get(base_url).text, 'html.parser'), base_url))

        for contact_page in contact_pages:
            contact_emails, contact_phones, contact_url, contact_page_type = scrape_page(contact_page, 'contact')
            emails.update(contact_emails)
            phones.update(contact_phones)

        return emails, phones, main_url, contact_pages

    # Set of URLs to scrape
    urls = set(df["web"])

    # Function to process each URL in parallel
    def process_url(url):
        visited_urls = set()
        all_results = []
        emails, phones, main_url, contact_pages = scrape_website_contacts(url, visited_urls=visited_urls)

        # Append the main page result
        all_results.append({
            'Base Website': url,
            'Scraped Page': main_url,
            'Page Type': 'main',
            'Emails': ', '.join(emails),
            'Phone Numbers': ', '.join(phones)
        })

        # Append the contact page results
        for contact_page in contact_pages:
            contact_emails, contact_phones, contact_url, contact_page_type = scrape_website_main_page(contact_page, page_type='contact')
            all_results.append({
                'Base Website': url,
                'Scraped Page': contact_url,
                'Page Type': contact_page_type,
                'Emails': ', '.join(contact_emails),
                'Phone Numbers': ', '.join(contact_phones)
            })

        return all_results

    # Use ThreadPoolExecutor to scrape URLs in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_url, url): url for url in urls}
        for future in tqdm(as_completed(futures), total=len(futures)):
            try:
                result = future.result()
                data.extend(result)
            except Exception as e:
                print(f"An error occurred: {e}")

    return data



def check_empty_or_nan(value):
    return pd.isna(value) or value == {} or value == [] or value == ''

def process_data(max_iterations=0):
    df = data_prep(subset=False)
    #df = df.iloc[200:230]
    scraped_dfs = []

    # Split the DataFrame into four parts
    df_split = np.array_split(df, 4)

    # Process each part separately
    for df_part in tqdm(df_split):
        result = scrape_urls(df_part)
        scraped_data = pd.DataFrame(result)
        scraped_dfs.append(scraped_data)

    # Concatenate the scraped data
    result_scraper = pd.concat(scraped_dfs).reset_index(drop=True)

    # Check for missing or empty emails or phone numbers
    missing_data = result_scraper[
        result_scraper["Emails"].apply(check_empty_or_nan) |
        result_scraper["Phone Numbers"].apply(check_empty_or_nan)
    ]["Base Website"]
    
    iterations = 0
    while len(missing_data) > 0 and iterations < max_iterations:
        print(f"Iteration {iterations + 1}: Found {len(missing_data)} websites with missing data. Scraping again.")
        set_not_found = list(set(missing_data))
        df = pd.DataFrame({"web": set_not_found})

        # Re-scrape the missing data
        missing_scraped_dfs = []
        df_split = np.array_split(df, 4)
        for df_part in tqdm(df_split):
            result = scrape_urls(df_part)
            scraped_data = pd.DataFrame(result)
            missing_scraped_dfs.append(scraped_data)
        
        # Concatenate the newly scraped data
        new_scraped_data = pd.concat(missing_scraped_dfs).reset_index(drop=True)
        
        # Merge the newly scraped data with the previous results
        result_scraper = result_scraper.reset_index(drop=True)
        result_scraper.update(new_scraped_data)

        # Check for remaining missing or empty emails or phone numbers
        missing_data = result_scraper[
            result_scraper["Emails"].apply(check_empty_or_nan) |
            result_scraper["Phone Numbers"].apply(check_empty_or_nan)
        ]["Base Website"]
        
        iterations += 1

    if iterations == max_iterations and len(missing_data) > 0:
        print(f"Reached maximum iterations ({max_iterations}). There are still {len(missing_data)} websites with missing data.")
    else:
        print("Scraping complete. No missing emails or phone numbers.")

    return result_scraper



###
# Phones Part
### 
def py_parse_phonenumber(num):
    try:
        parsed_num = phonenumbers.parse(num, 'CZ')
        phonenumbers.is_possible_number_with_reason(parsed_num)
        return {
            'formated_number':phonenumbers.format_number(parsed_num, phonenumbers.PhoneNumberFormat.E164),
            'number': parsed_num.national_number,
            'prefix': parsed_num.country_code,
            'country_code': phonenumbers.region_code_for_number(parsed_num),
            'valid': phonenumbers.is_valid_number(parsed_num),
            'possible': phonenumbers.is_possible_number(parsed_num),
            'parsed': True
        }
    except Exception as e:
        return {'number': num, 'prefix': None, 'country_code': None, 'valid': False, 'possible': False, 'parsed': False}

def udf(df: pd.DataFrame, column_name: str):
    results = df[column_name].apply(py_parse_phonenumber)
    parsed_df = pd.DataFrame(results.tolist())
    return parsed_df

def explode_df(df:pd.DataFrame, column_name:str) -> pd.DataFrame:
    #df_res = pd.read_csv("../data/1_06/df_scraped_full.csv")
    df_res = df
    df_res[f'{column_name}_scraped'] = df_res[column_name].str.split(', ')
   # df_res['emails_scraped'] = df_res['Emails'].str.split(', ')
    df_res_phones = df_res[["Base Website", "Scraped Page",f"{column_name}_scraped"]]
    df_res_exp = df_res_phones.explode(f'{column_name}_scraped').reset_index(drop=True)
    return df_res_exp

def has_more_than_5_consecutive_zeros(number):
    return bool(re.search(r'0{4,}', str(number)))


def clean_scraped_phones(df: pd.DataFrame, phone_scraped_column = "Phone Numbers") -> pd.DataFrame:
    phones_exp = explode_df(df,phone_scraped_column)
    ress_df = udf(phones_exp,f"{phone_scraped_column}_scraped")
    phones_exp["formated_number"] = ress_df["formated_number"]
    phones_exp.drop(columns=[f"{phone_scraped_column}_scraped"], inplace= True)
    phones_deduped = phones_exp.drop_duplicates(subset=['Base Website', 'formated_number'])
    filtered_df = phones_deduped[~phones_deduped['formated_number'].apply(has_more_than_5_consecutive_zeros)]
    phones_df = filtered_df.sort_values(by="formated_number", ascending=True)
    return phones_df

# Emails Part 
def clean_email(email):
    if pd.isna(email):
        return email
    # Regular expression to find valid email addresses
    email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b')
    # Find all valid email addresses in the string and convert them to lowercase
    valid_emails = [e.lower() for e in email_pattern.findall(email)]
    # Fix emails that have additional symbols after specified domains
    domains = ['.cz', '.com', '.eu', '.org']
    cleaned_emails = []
    for email in valid_emails:
        for domain in domains:
            if email.endswith(domain):
                email = re.sub(f'{domain}.*', domain, email)
                break
        cleaned_emails.append(email)
    return ', '.join(cleaned_emails)

def clean_scraped_emails(df: pd.DataFrame, email_scraped_column = "Emails") -> pd.DataFrame:
    emails_exp = explode_df(df,email_scraped_column)
    emails_exp[f'{email_scraped_column}_scraped'] = emails_exp[f'{email_scraped_column}_scraped'].apply(clean_email)
    emails_deduped = emails_exp.drop_duplicates(subset=['Base Website', f'{email_scraped_column}_scraped'])
    # emails_sorted = emails_deduped.sort_values(by="Emails_scraped", ascending=False)
    return emails_deduped

def db_pomoci_transform(df:pd.DataFrame) -> pd.DataFrame:
    """
    This function prepare data initial data for validation
    """
    df['E-mail'] = df['E-mail'].apply(clean_email)
    ress_df = udf(df,"Telefon")
    df['Telefon'] = ress_df["formated_number"]
    df = df[~df["Webová stránka"].isna()]
    df.loc[df['Webová stránka'].str.startswith('www'), 'web'] = df['Webová stránka'].str.replace('^www', 'https://www', regex=True)
    return df


def main():
    # Execute the process
    result_scraper = process_data()
    result_scraper.to_csv(f'{SAVE_FILES_PATH}df_scraped_{datetime.datetime.now().day}_{datetime.datetime.now().strftime("%m")}.csv')
    df_phones_scraped = clean_scraped_phones(result_scraper)
    df_emails_scraped = clean_scraped_emails(result_scraper)
    df_emails_scraped = df_emails_scraped[['Base Website', 'Scraped Page', 'Emails_scraped']]
    df_emails_scraped['Contact_type'] = 'Email'
    df_emails_scraped.rename(columns={'Emails_scraped': 'Contact'}, inplace=True)

    df_phones_scraped = df_phones_scraped[['Base Website', 'Scraped Page', 'formated_number']]
    df_phones_scraped['Contact_type'] = 'Phone'
    df_phones_scraped.rename(columns={'formated_number': 'Contact'}, inplace=True)

    # Combine the DataFrames by appending rows
    combined_df = pd.concat([df_phones_scraped, df_emails_scraped], ignore_index=True)
    combined_df.to_csv(SCRAPED_DATA_PATH, index=False)
    combined_df.shape
    db_pomoci = pd.read_csv(f"{DB_POMOCI_PATH}")
    db_pomoci = db_pomoci_transform(db_pomoci)
    maps_results = pd.read_csv(MAPS_SCRAPED, delimiter=";")
    maps_contacts = pd.concat([
    maps_results[['Email']].rename(columns={'Email': 'Contact'}),
    maps_results[['API Phone']].rename(columns={'API Phone': 'Contact'})
    ]).dropna().drop_duplicates()

    # Extract contacts from combined_df
    scraped_contacts = combined_df[['Contact']].dropna().drop_duplicates()

    def check_contact(contact, contacts_df, source_name):
        """
        Check if a contact exists in the given contacts DataFrame.
        """
        if contact in contacts_df['Contact'].values:
            return source_name
        return None

    def match_contact(row, maps_contacts, scraped_contacts):
        """
        Match contact details in the row with known contact sources.
        """
        sources = set()

        # Check against maps contacts
#         if check_contact(row['E-mail'], maps_contacts, 'maps_contacts'):
#             sources.add('maps_contacts_e_mail')
        if check_contact(row['Telefon'], maps_contacts, 'maps_contacts'):
            sources.add('maps_contacts_telefon')

        # Check against scraped contacts
        if check_contact(row['E-mail'], scraped_contacts, 'scraped_contacts'):
            sources.add('scraped_contacts_email')
        if check_contact(row['Telefon'], scraped_contacts, 'scraped_contacts'):
            sources.add('scraped_contacts_telefon')

        if sources:
            return 'matched', ', '.join(sources)
        return 'unmatched', None
    
    db_pomoci[['Matched', 'Source']] = db_pomoci.apply(
    lambda row: pd.Series(match_contact(row, maps_contacts, scraped_contacts)), axis=1)

#     # Check and flag contacts in db_pomoci
#     db_pomoci['Matched'] = db_pomoci.apply(
#         lambda row: 'matched' if check_contact(row['E-mail'], maps_contacts) or 
#                             check_contact(row['Telefon'], maps_contacts) or 
#                             check_contact(row['E-mail'], scraped_contacts) or 
#                             check_contact(row['Telefon'], scraped_contacts) 
#                     else 'unmatched', axis=1)
    matched_num = db_pomoci[db_pomoci["Matched"]=="matched"].shape[0]
    unmatched_num = db_pomoci[db_pomoci["Matched"]=="unmatched"].shape[0]
    print(f"Data baze obsahuje {matched_num} schodnych kontaktu a {unmatched_num} neschodnych kontaktu")
    db_pomoci.to_csv(FLAGGED_DATA_PATH, index=False)        
    

if __name__ == "__main__":
    main()    







# Example usage
# df = pd.DataFrame({"web": ["https://www.psychologie-praha3.cz/"]})

# result = scrape_urls(df)
# print(result)                   
