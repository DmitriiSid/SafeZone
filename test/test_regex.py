import pandas as pd
import requests
import pandas as pd
from bs4 import BeautifulSoup
import re



phone_regex = re.compile(r"""
    (?:
        \+420[-\s]?       # Optional country code +420 followed by an optional space or dash
    )?
    (?:                  # Non-capturing group for the phone number
        \d{3}[-\s]?      # First part of the phone number (3 digits)
        \d{3}[-\s]?      # Second part of the phone number (3 digits)
        \d{3}            # Third part of the phone number (3 digits)
        (?:\d{3})?       # Optional fourth part of the phone number (3 digits)
    )
""", re.VERBOSE)
email_regex = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")

# Test the regex
test_numbers = [
    "+420 123 456 789",
    "+420-123-456-789",
    "123 456 789",
    "123 456 789 123",
    "123456789123"
]

for number in test_numbers:
    match = phone_regex.search(number)
    if match:
        print(f"Matched: {match.group()}")
    else:
        print(f"No match: {number}")


test_emails = ['cheiront@cheiront.cz']

for email in test_emails:
    match = email_regex.search(email)
    if match:
        print(f"Matched: {match.group()}")
    else:
        print(f"No match: {email}")