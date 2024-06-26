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

pd.options.mode.chained_assignment = None

def has_more_than_5_consecutive_zeros(number):
    return bool(re.search(r'0{4,}', str(number)))

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

def explode_df(df:pd.DataFrame, column_name:str, web_column = "Base Website", scraped_web_column = "Scraped Page") -> pd.DataFrame:
    #df_res = pd.read_csv("../data/1_06/df_scraped_full.csv")
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


def clean_scraped_phones(df: pd.DataFrame, phone_scraped_column = "Phone Numbers", web_column = "Base Website",scraped_web_column = "Scraped Page") -> pd.DataFrame:
    phones_exp = explode_df(df,phone_scraped_column,web_column,scraped_web_column)
    ress_df = udf(phones_exp,f"{phone_scraped_column}_scraped")
    phones_exp["formated_number"] = ress_df["formated_number"]
    phones_exp.drop(columns=[f"{phone_scraped_column}_scraped"], inplace= True)
    phones_deduped = phones_exp.drop_duplicates(subset=[web_column, 'formated_number'])
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

def clean_scraped_emails(df: pd.DataFrame, email_scraped_column = "Emails", web_column = "Base Website") -> pd.DataFrame:
    emails_exp = explode_df(df,email_scraped_column)
    emails_exp[f'{email_scraped_column}_scraped'] = emails_exp[f'{email_scraped_column}_scraped'].apply(clean_email)
    emails_deduped = emails_exp.drop_duplicates(subset=[web_column, f'{email_scraped_column}_scraped'])
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

db_pomoci_flagged_new_V2 = pd.read_csv("db_pomoci_flagged_new_V2.csv")
db_pomoci = pd.read_csv("db_pomoci.csv")
result_scraper = pd.read_csv("df_scraped_2_06.csv")
maps_results = pd.read_csv("maps_results.csv", sep=";")


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
print(combined_df.head())

db_pomoci = db_pomoci_transform(db_pomoci)

maps_results = clean_scraped_phones(maps_results,"API Phone", web_column = "Web", scraped_web_column =  "Web")
print(maps_results.columns)
#maps_results = clean_scraped_emails(maps_results, "Email")

maps_contacts = pd.concat([
    #maps_results[['Email']].rename(columns={'Email': 'Contact'}),
    maps_results[['Web','formated_number']].rename(columns={'formated_number': 'Contact'})
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
maps_contacts.to_csv('maps_contacts.csv')

# def find_new_contact(row):
#     if row['Matched'] == 'unmatched':
#         web = row['Webová stránka']
#         maps_contact = maps_contacts[maps_contacts['Web'].str.contains(web, na=False)]
#         scraped_contact = combined_df[combined_df['Base Website'].str.contains(web, na=False)]
        
#         if not maps_contact.empty and not scraped_contact.empty:
#             new_contact = maps_contact.iloc[0]['Contact']
#             return pd.Series([new_contact, 'new_contact_both_match'])
#         elif not maps_contact.empty:
#             new_contact = maps_contact.iloc[0]['Contact']
#             return pd.Series([new_contact, 'new_contact_scraped_maps'])
#         elif not scraped_contact.empty:
#             new_contact = scraped_contact.iloc[0]['Contact']
#             return pd.Series([new_contact, 'new_contact_scraped_combined'])
#     return pd.Series([None, None])
def find_new_contact(row):
    if row['Matched'] == 'unmatched':
        web = row['Webová stránka']
        maps_contact = maps_contacts[maps_contacts['Web'].str.contains(web, na=False)]
        scraped_contact = combined_df[combined_df['Base Website'].str.contains(web, na=False)]
        
        # Find common contacts in both maps_contact and scraped_contact
        common_contacts = pd.merge(maps_contact, scraped_contact, on='Contact')
        scraped_email_contact = combined_df[(combined_df['Base Website'].str.contains(web, na=False)) & (combined_df["Contact_type"] == "Email")]
        new_contact = []
        

        if not common_contacts.empty and not scraped_email_contact.empty:
            new_contact.extend(common_contacts['Contact'].values)
            new_contact.extend(scraped_email_contact['Contact'].values)
            return pd.Series([new_contact, 'new_contact_both_match_with_email'])
        if not common_contacts.empty  and  scraped_email_contact.empty:
            new_contact.extend(common_contacts['Contact'].values)
            return pd.Series([new_contact, 'new_contact_both_match'])
         if  common_contacts.empty  and  not scraped_email_contact.empty:
            new_contact.extend(scraped_email_contact['Contact'].values)
            return pd.Series([new_contact, 'new_contact_both_match'])
            
        # if not common_contacts.empty:
        #     new_contact.extend(common_contacts['Contact'].values)
            
        # if scraped_email_contact.empty and  len(new_contact) <0:
        #     return pd.Series([new_contact, 'new_contact_both_match'])
            
        # if not scraped_email_contact.empty:
        #     new_contact.extend(scraped_email_contact['Contact'].values)
            
        # if new_contact:
        #     return pd.Series([new_contact, 'new_contact_combined'])
    return pd.Series([None, None])
    # if not common_contacts.empty:
        #     # new_contact = common_contacts.iloc[0]['Contact'] #???
        #     new_contact = common_contacts['Contact'].values #???
        #     return pd.Series([new_contact, 'new_contact_both_match'])


db_pomoci[['New Contact', 'New Matched']] = db_pomoci.apply(find_new_contact, axis=1)