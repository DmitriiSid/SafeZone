import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from tqdm import tqdm
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

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
                print(url)
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
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(process_url, url): url for url in urls}
        for future in tqdm(as_completed(futures), total=len(futures)):
            try:
                result = future.result()
                data.extend(result)
            except Exception as e:
                print(f"An error occurred: {e}")

    return data

# Example usage
#df = pd.DataFrame({"web": ["http://www.pho.cz/"]})


                   
