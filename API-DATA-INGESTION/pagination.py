# Task: Fetch all pages of data from a paginated API.

import requests
import pandas as pd
import time

def fetch_all_pages(base_url):
    all_data = []
    page = 1

    while True:
        print(f'Fetching page {page}...')
        params = {'_page': page, '_limit': 10 }
        response = requests.get(base_url, params=params)

        if response.status_code != 200:
            print(f"Error on page {page}: {response.status_code}")
            break
        data = response.json()

        if not data:
            print("No more data")
            break

        all_data.extend(data)
        print(f"Retrieved {len(data)} items")

        page += 1
        time.sleep(0.5)
    return all_data

url = 'https://jsonplaceholder.typicode.com/posts'
all_posts = fetch_all_pages(url)
df = pd.DataFrame(all_posts)
print(f"\nTotal posts fetched: {len(df)}")
print(df.head())

duplicates = df.duplicated(subset=['id']).sum()
print(f"Duplicate records: {duplicates}")