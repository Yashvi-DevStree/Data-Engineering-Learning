import requests
import pandas as pd
from datetime import datetime
import os

def generate_filename():
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f'users_{timestamp}.csv'

def fetch_and_save_users():
    print("Starting Daily User Data Extraction")

    api_url = 'https://jsonplaceholder.typicode.com/users'
    output_dir = 'data'
    os.makedirs(output_dir, exist_ok=True)

    try:
        # Step 1: fetch data from api
        print(f"\n[1/3] Fetching data from API...")
        response = requests.get(api_url, timeout=10)

        # Check if request was successful
        if response.status_code == 200:
            print("✓ Successfully fetched data")
        else:
            print(f"✗ API returned error (Status: {response.status_code})")
            return
        
        #  Step 2: Parse JSON data
        print(f"\n[2/3] Processing data...")
        user_data = response.json()

        if not user_data:
            print("No data returned from API")
            return 
        
        print(f"✓ Received {len(user_data)} user records")

        # Convert to DataFrame
        df = pd.DataFrame(user_data)

        # Flatten nested address data
        df['city'] = df['address'].apply(lambda x: x['city'])
        df['street'] = df['address'].apply(lambda x: x['street'])
        
        # Select only needed columns
        df_clean = df[['id', 'name', 'username', 'email', 'phone', 'city', 'street']]

        # Step 3: Save to CSV
        print(f"\n[3/3] Saving to CSV...")
        filename = generate_filename()
        filepath = os.path.join(output_dir, filename)

        df_clean.to_csv(filepath, index=False)
        print("SUCCESS: Daily user extraction completed")

    except requests.exceptions.Timeout:
        print("\n✗ ERROR: Request timed out")
        print("  The API took too long to respond")
    
    except requests.exceptions.ConnectionError:
        print("\n✗ ERROR: Connection failed")
        print("  Could not connect to the API")

    except requests.exceptions.RequestException as e:
        print(f"\n✗ ERROR: API request failed")
        print(f"  Details: {str(e)}")

if __name__ == "__main__":
    fetch_and_save_users()

    

