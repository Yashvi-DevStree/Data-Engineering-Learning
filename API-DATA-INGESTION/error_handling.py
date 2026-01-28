# Task: Implement robust error handling with retry logic.
# Create a function that:
# 1. Tries to fetch data from an API
# 2. Retries up to 3 times on failure
# 3. Uses exponential backoff
# 4. Handles different error types appropriately

import requests
import time
from requests.exceptions import Timeout, ConnectionError, HTTPError

def fetch_with_retry(url, max_retries = 3, timeout = 10):
    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt + 1} of {max_retries}")
            response = requests.get(url, timeout=timeout)

            # Raise exception for bad status code
            response.raise_for_status()

            print("✓ Request successful")
            return response.json()
        
        except Timeout:
            print(f"✗ Timeout error on attempt {attempt + 1}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt     # Exponential backoff: 1s, 2s, 4s
                print(f"  Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)

        except ConnectionError:
            print(f"✗ Connection error on attempt {attempt + 1}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"  Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)

        except HTTPError as e:
            status_code = e.response.status_code

            if status_code == 429:  
                print("✗ Rate limit exceeded")
                retry_after = e.response.headers.get('Retry-After', 60)
                print(f"  Waiting {retry_after} seconds...")
                time.sleep(int(retry_after))

            elif status_code == 500:
                print(f"✗ Server error: {status_code}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    print(f"  Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)

            else:
                print(f"✗ Client error: {status_code}")
                print(f"  Error message: {e.response.text}")
                return None
            
        except Exception as e:
            print(f"✗ Unexpected error: {str(e)}")
            return None
    
    print("✗ All retry attempts failed")
    return None

print("Testing with valid URL:")
data = fetch_with_retry('https://jsonplaceholder.typicode.com/users')
if data:
    print(f"Successfully fetched {len(data)} records\n")

# Test with invalid URL (will fail)
# print("Testing with invalid URL:")
# data = fetch_with_retry('https://jsonplaceholder.typicode.com/invalid_endpoint')