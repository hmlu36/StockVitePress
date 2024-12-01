import os
import time
import random
import requests
import pyuser_agent
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO

def get_headers():
    """Generate random user-agent headers."""
    ua = pyuser_agent.UA()
    user_agent = ua.random
    headers = {"user-agent": user_agent}
    return headers

def sleep():
    """Sleep for a random interval between 3 and 10 seconds."""
    time.sleep(random.randint(3, 10))

def get_root_path():
    """Get the root directory path of the current script."""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def get_dataframe_by_css_selector(url, css_selector):
    """
    Fetch HTML content from a URL, parse it using a CSS selector, and return a DataFrame.

    Parameters:
    url (str): The URL to fetch the HTML content from.
    css_selector (str): The CSS selector to locate the desired HTML element.

    Returns:
    pd.DataFrame: The parsed data as a DataFrame.
    """
    headers = get_headers()
    
    try:
        response = requests.get(url, headers=headers, timeout=(5, 10))
        response.raise_for_status()  # Raise an exception for HTTP errors
    except requests.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return pd.DataFrame()

    response.encoding = "utf-8"
    soup = BeautifulSoup(response.text, "html.parser")
    data = soup.select_one(css_selector)
    
    if not data:
        print(f"No data found for CSS selector: {css_selector}")
        return pd.DataFrame()

    try:
        dfs = pd.read_html(StringIO(data.prettify()))
    except ValueError as e:
        print(f"Error parsing HTML to DataFrame: {e}")
        return pd.DataFrame()

    for df in dfs:
        if len(df) > 1:
            return df

    return pd.DataFrame()