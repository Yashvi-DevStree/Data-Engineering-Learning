# Task: Fetch data from an API that requires authentication.

import requests
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv('API_KEY', 'demo_key')

def fetch_with_api_key(url, api_key):
    headers = {
        'X-API-Key': api_key,
        'Content-Type': 'application/json',
        'User-Agent': 'DataPipeline/1.0'
    }

    try:
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 400:
            print("Authentication failed. Invalid API Key")
        elif response.status_code == 403:
            print("Forbidden: API Key does't have required permission.")
        else:
            print(f"Error: {response.status_code}")
            print(f"Message: {response.text}")
        return None
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

def fetch_with_bearer_token(url, token):
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        return None


nasa_url = 'https://api.nasa.gov/planetary/apod'
params = {
    'api_key': 'DEMO_KEY',  
    'date': '2024-01-15'
}

response = requests.get(nasa_url, params=params)
if response.status_code == 200:
    apod_data = response.json()
    print("Astronomy Picture of the Day:")
    print(f"Title: {apod_data.get('title')}")
    print(f"Date: {apod_data.get('date')}")
    print(f"Explanation: {apod_data.get('explanation')[:100]}...")