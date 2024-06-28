import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import re
from tqdm import tqdm 
import phonenumbers
import phonenumbers.geocoder
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import datetime
import os

RUN_EVIRONMENT = "keboola"

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
    DB_POMOCI_PATH = "in/tables/db_pomoci.csv"
    MAX_WORKERS = 8
    SAVE_FILES_PATH = "out/tables/"
    pass
else:
    raise EnvironmentError("this environment is not supported")

    
def ensure_url_format(url):
    """
    Zajisti, aby URL melo spravny format, pokud nezacina na 'www', 'http' nebo 'https', prida 'www.'

    Parametry:
    url (str): URL adresa k overeni

    Navratova hodnota:
    str: URL adresa ve spravnem formatu
    """
    if not (url.startswith('www') or url.startswith('http') or url.startswith('https')):
        return 'www.' + url
    return url


def data_prep(subset:bool = False) -> pd.DataFrame:
    """
    Pripravi data pro skript, odstrani nepouzitelne odkazy a upravi format webovych stranek.

    Parametry:
    subset (bool): Pokud je True, vrati pouze prvnich 100 radku, pouziva se pro trouble shooting

    Navratova hodnota:
    pd.DataFrame: Upraveny DataFrame s pripravenymi daty
    """

    db_pomoci = pd.read_csv(DB_POMOCI_PATH)
    df = db_pomoci.rename(columns={"Webova_stranka" : "web", "Nazev":"nazev","E_mail":"email"})
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

def validate_url(url):
    """
    Overi, zda URL obsahuje scheme (http/https), pokud ne, prida 'https://'

    Parametry:
    url (str): URL adresa k overeni

    Navratova hodnota:
    str: URL adresa se spravnym schematem
    """

    parsed_url = urlparse(url)
    if not parsed_url.scheme:
        return "https://" + url
    return url


def scrape_urls(df):
    """
    Skriptuje URL adresy z DataFrame a hleda emaily a telefonni cisla.

    Parametry:
    df (pd.DataFrame): DataFrame obsahujici URL adresy k prohledavani

    Navratova hodnota:
    list: List slovniku s nalezenymi emaily a telefonni cisly pro kazdou URL adresu
    """

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
        """
        Skriptuje hlavni stranku webu a hleda emaily a telefonni cisla.

        Parametry:
        url (str): URL adresa k prohledani
        retries (int): Pocet pokusu pri selhani
        backoff_factor (float): Faktor pro exponentialni cekani mezi pokusy
        page_type (str): Typ stranky ('main' nebo 'contact')

        Navratova hodnota:
        tuple: Sada nalezenych emailu, telefonnich cisel, URL adresa a typ stranky
        """

        url = validate_url(url)
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
        """
        Skriptuje potencialni kontaktni stranky webu a hleda emaily a telefonni cisla.

        Parametry:
        base_url (str): Zakladni URL adresa webu
        visited_urls (set): Sada navstivenych URL adres
        retries (int): Pocet pokusu pri selhani
        backoff_factor (float): Faktor pro exponentialni cekani mezi pokusy

        Navratova hodnota:
        tuple: Sada nalezenych emailu, telefonnich cisel, zakladni URL adresa a kontaktni stranky
        """

        contact_pages = set()
        emails = set()
        phones = set()

        def get_contact_links(soup, base_url):
            """
            Najde odkazy na kontaktni stranky v HTML dokumentu.

            Parametry:
            soup (BeautifulSoup): Parsovany HTML dokument
            base_url (str): Zakladni URL adresa webu

            Navratova hodnota:
            list: List URL adres kontaktovanych stranek
            """

            links = []
            for link in soup.find_all('a', href=True):
                if 'kontakt' in link.text.lower() or 'contact' in link.text.lower() or 'kontakty' in link.text.lower() or 'o nás' in link.text.lower()  or 'kdo-jsem' in link.text.lower() or 'o-nas' in link.text.lower():
                    contact_href = urljoin(base_url, link['href'])
                    if contact_href not in visited_urls:
                        links.append(contact_href)
            return links

        def scrape_page(url, page_type):
            """
            Skriptuje stranku a hleda emaily a telefonni cisla.

            Parametry:
            url (str): URL adresa k prohledani
            page_type (str): Typ stranky ('main' nebo 'contact')

            Navratova hodnota:
            tuple: Sada nalezenych emailu, telefonnich cisel, URL adresa a typ stranky
            """

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
        """
        Zpracuje URL adresu, skriptuje hlavni a kontaktni stranky a hleda emaily a telefonni cisla.

        Parametry:
        url (str): URL adresa k prohledani

        Navratova hodnota:
        list: List slovniku s nalezenymi emaily a telefonni cisly pro kazdou URL adresu
        """
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
    """
    Overi, zda je hodnota prazdna nebo NaN.

    Parametry:
    value: Hodnota k overeni

    Navratova hodnota:
    bool: True, pokud je hodnota prazdna nebo NaN, jinak False
    """
    return pd.isna(value) or value == {} or value == [] or value == ''

def process_data(max_iterations=0):
    """
    Zpracuje data, skriptuje URL adresy a opakovane kontroluje chybejici data.

    Parametry:
    max_iterations (int): Maximalni pocet opakovani pri hledani chybejicich dat

    Navratova hodnota:
    pd.DataFrame: DataFrame s nalezenymi daty po skriptovani
    """
    df = data_prep(subset=False)
    scraped_dfs = []
    
    df_split = np.array_split(df, 4)

    for df_part in tqdm(df_split):
        result = scrape_urls(df_part)
        scraped_data = pd.DataFrame(result)
        scraped_dfs.append(scraped_data)

    result_scraper = pd.concat(scraped_dfs).reset_index(drop=True)

    missing_data = result_scraper[
        result_scraper["Emails"].apply(check_empty_or_nan) |
        result_scraper["Phone Numbers"].apply(check_empty_or_nan)
    ]["Base Website"]
    
    iterations = 0
    while len(missing_data) > 0 and iterations < max_iterations:
        print(f"Iteration {iterations + 1}: Found {len(missing_data)} websites with missing data. Scraping again.")
        set_not_found = list(set(missing_data))
        df = pd.DataFrame({"web": set_not_found})

        missing_scraped_dfs = []
        df_split = np.array_split(df, 4)
        for df_part in tqdm(df_split):
            result = scrape_urls(df_part)
            scraped_data = pd.DataFrame(result)
            missing_scraped_dfs.append(scraped_data)
        
        new_scraped_data = pd.concat(missing_scraped_dfs).reset_index(drop=True)
        
        result_scraper = result_scraper.reset_index(drop=True)
        result_scraper.update(new_scraped_data)

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
    """
    Analyzuje telefonni cislo a vraci informace o jeho formatu, platnosti a moznosti.

    Parametry:
    num (str): Telefonni cislo k analyzovani

    Navratova hodnota:
    dict: Slovnik s informacemi o telefonni cislo
    """
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
    """
    Pouziva funkci py_parse_phonenumber na DataFrame sloupec a vraci novy DataFrame s analyzovanymi cisly.

    Parametry:
    df (pd.DataFrame): DataFrame obsahujici telefonni cisla
    column_name (str): Nazev sloupce s telefonni cisly

    Navratova hodnota:
    pd.DataFrame: DataFrame s analyzovanymi telefonni cisly
    """
    results = df[column_name].apply(py_parse_phonenumber)
    parsed_df = pd.DataFrame(results.tolist())
    return parsed_df

def explode_df(df:pd.DataFrame, column_name:str, web_column = "Base Website", scraped_web_column = "Scraped Page") -> pd.DataFrame:
    """
    Rozdeli hodnoty ve sloupci DataFrame a vytvori novy DataFrame s rozdelenymi hodnotami.

    Parametry:
    df (pd.DataFrame): DataFrame s hodnotami k rozdeleni
    column_name (str): Nazev sloupce k rozdeleni
    web_column (str): Nazev sloupce s zakladnimi URL adresami
    scraped_web_column (str): Nazev sloupce se skriptovanymi URL adresami

    Navratova hodnota:
    pd.DataFrame: Novy DataFrame s rozdelenymi hodnotami
    """
    df_res = df
    df_res[f'{column_name}_scraped'] = df_res[column_name].str.split(', ')
    # df_res['emails_scraped'] = df_res['Emails'].str.split(', ')
    if scraped_web_column != "Scraped Page":
        df_res_phones = df_res[[web_column,f"{column_name}_scraped"]]
        df_res_exp = df_res_phones.explode(f'{column_name}_scraped').reset_index(drop=True)
        return df_res_exp
    df_res_phones = df_res[[web_column, scraped_web_column,f"{column_name}_scraped"]]
    df_res_exp = df_res_phones.explode(f'{column_name}_scraped').reset_index(drop=True)
    return df_res_exp

def has_more_than_3_consecutive_zeros(number):
    """
    Overi, zda telefonni cislo obsahuje vice nez 3 po sobe jdoucich nul.

    Parametry:
    number (str): Telefonni cislo k overeni

    Navratova hodnota:
    bool: True, pokud cislo obsahuje vice nez 3 po sobe jdoucich nul, jinak False
    """
    return bool(re.search(r'0{4,}', str(number)))


def clean_scraped_phones(df: pd.DataFrame, phone_scraped_column = "Phone Numbers", web_column = "Base Website",scraped_web_column = "Scraped Page") -> pd.DataFrame:
    """
    Cisti a formatuje telefonni cisla nalezena pri skriptovani.

    Parametry:
    df (pd.DataFrame): DataFrame s telefonni cisly
    phone_scraped_column (str): Nazev sloupce s telefonni cisly
    web_column (str): Nazev sloupce s zakladnimi URL adresami
    scraped_web_column (str): Nazev sloupce se skriptovanymi URL adresami

    Navratova hodnota:
    pd.DataFrame: Cisteny a formatovany DataFrame s telefonni cisly
    """
    phones_exp = explode_df(df,phone_scraped_column,web_column,scraped_web_column)
    ress_df = udf(phones_exp,f"{phone_scraped_column}_scraped")
    phones_exp["formated_number"] = ress_df["formated_number"]
    phones_exp.drop(columns=[f"{phone_scraped_column}_scraped"], inplace= True)
    phones_deduped = phones_exp.drop_duplicates(subset=[web_column, 'formated_number'])
    filtered_df = phones_deduped[~phones_deduped['formated_number'].apply(has_more_than_3_consecutive_zeros)]
    phones_df = filtered_df.sort_values(by="formated_number", ascending=True)
    return phones_df

# Emails Part 
def clean_email(email):
    """
    Cisti a formatuje emailove adresy.

    Parametry:
    email (str): Emailova adresa k cisteni

    Navratova hodnota:
    str: Cistena a formatovana emailova adresa
    """
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

def clean_scraped_emails(df: pd.DataFrame, email_scraped_column = "Emails", web_column = "Base Website") -> pd.DataFrame:
    """
    Cisti a formatuje emailove adresy nalezene pri skriptovani.

    Parametry:
    df (pd.DataFrame): DataFrame s emailovymi adresami
    email_scraped_column (str): Nazev sloupce s emailovymi adresami
    web_column (str): Nazev sloupce s zakladnimi URL adresami

    Navratova hodnota:
    pd.DataFrame: Cisteny a formatovany DataFrame s emailovymi adresami
    """
    emails_exp = explode_df(df,email_scraped_column)
    emails_exp[f'{email_scraped_column}_scraped'] = emails_exp[f'{email_scraped_column}_scraped'].apply(clean_email)
    emails_deduped = emails_exp.drop_duplicates(subset=[web_column, f'{email_scraped_column}_scraped'])
    # emails_sorted = emails_deduped.sort_values(by="Emails_scraped", ascending=False)
    return emails_deduped

def db_pomoci_transform(df:pd.DataFrame) -> pd.DataFrame:
    """
    Pripravuje pocatecni data pro validaci.

    Parametry:
    df (pd.DataFrame): DataFrame s daty k transformaci

    Navratova hodnota:
    pd.DataFrame: Upraveny DataFrame s pripravenymi daty
    """
    df['E_mail'] = df['E_mail'].apply(clean_email)
    ress_df = udf(df,"Telefon")
    df['Telefon'] = ress_df["formated_number"]
    df = df[~df["Webova_stranka"].isna()]
    df.loc[df['Webova_stranka'].str.startswith('www'), 'web'] = df['Webova_stranka'].str.replace('^www', 'https://www', regex=True)
    return df
  
def main():
    """
    Hlavni funkce pro zpracovani a skriptovani dat. Vysledky uklada do CSV souboru.
    """
    result_scraper = process_data()
    result_scraper.to_csv(f'out/tables/df_scraped.csv', index=False)

main()