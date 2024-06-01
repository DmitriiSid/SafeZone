import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import re
from tqdm import tqdm 
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import datetime

pd.set_option('display.max_rows', 500)

def data_prep(subset:bool = False) -> pd.DataFrame:
    db_pomoci = pd.read_csv("db_pomoci.csv")
    df = db_pomoci.rename(columns={"Webová stránka" : "web", "Název":"nazev","E-mail":"email"})
    black_list = ['http://www.dc-brno.cz','www.freeklub.cz', 'http://www.cszs.cz/','http://www.fokustabor.cz/centrum-dusevniho-zdravi-_-komunitni-tym-tabor']
    df = df[(~df["web"].isna()) & (~df['web'].isin(black_list))]
    df['web'] = df['web'].str.replace(r'\s+', '', regex=True)
    df.loc[df['web'].str.contains(r'\.cz/.+'), 'web'] = df['web'].str.replace(r'\.cz/.+', '.cz', regex=True)
    df.loc[df['web'].str.startswith('www'), 'web'] = df['web'].str.replace('^www', 'https://www', regex=True)
    df_agg_tel = df.groupby("web")["Telefon"].agg(list).reset_index()
    df_agg_email = df.groupby("web")["email"].agg(list).reset_index()
    df_agg = pd.merge(df_agg_tel, df_agg_email, on="web")#.drop("index", axis= 1)
    df_agg = df_agg.rename(columns={'web': 'web', 'Telefon': 'Telefon'})
    df_agg = pd.DataFrame(df_agg)
    if subset:
        df_agg = df_agg.head(100) # ubset first 300 rows
    return df_agg


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
    with ThreadPoolExecutor(max_workers=2) as executor:
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

def process_data(max_iterations=1):
    df = data_prep(subset=False)
    #df = df.iloc[200:220]
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

# Execute the process
result_scraper = process_data()
result_scraper.to_csv(f'data/df_scraped_{datetime.datetime.now().day}_{datetime.datetime.now().strftime("%m")}.csv')



# Example usage
# df = pd.DataFrame({"web": ["https://www.psychologie-praha3.cz/"]})

# result = scrape_urls(df)
# print(result)                   
