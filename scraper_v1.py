import requests
import pandas as pd
from bs4 import BeautifulSoup
import re

def main():
    db_pomoci = pd.read_csv("db_pomoci.csv")
    
    # Define the list of URLs to scrape
    urls = [
        "http://www.pdz.cz",
        "http://www.cdzopava.cz/",
        "http://dusevnizdravi.com/centrum-dusevniho-zdravi/",
        "https://mudr-vaclav-ferus.zdravotniregistr.cz/",
        "http://www.ordinace.cz/ordinace/vo/design_10/profil.php?page=lekar&id=38061",
        "http://www.modredvere.cz",
        "https://www.nempt.cz/ambulance/soukrome-ambulance/psychiatricka-ambulance/",
        "https://www.dagmarhruba.cz/cs/"
    ]

    # Regular expressions for matching emails and phone numbers
    email_regex = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
    phone_regex = re.compile(r"\+?(\d[\d\-\(\) ]{9,}\d)")

    # List to collect data
    data = []

    # Function to scrape a single URL
    def scrape_website(url):
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raises an HTTPError if the response status code is 4XX or 5XX
            soup = BeautifulSoup(response.text, 'html.parser')

            emails = set(email_regex.findall(soup.text))
            phones = set(phone_regex.findall(soup.text))

            return emails, phones
        except requests.RequestException as e:
            print(f"Error fetching {url}: {e}")
            return set(), set()

    # Loop through the URLs and scrape each one
    for url in urls:
        emails, phones = scrape_website(url)
        # Convert sets to comma-separated strings for storage
        emails_str = ', '.join(emails)
        phones_str = ', '.join(phones)
        
        # Collect data
        data.append({'Website': url, 'Emails': emails_str, 'Phone Numbers': phones_str})

    # Create DataFrame from collected data
    df = pd.DataFrame(data)

    print(df)

main()