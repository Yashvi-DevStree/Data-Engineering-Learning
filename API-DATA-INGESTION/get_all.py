# Task: Fetch data from a public API and convert it to a Pandas DataFrame.
import requests
import pandas as pd

# Fetch data from API
url = 'https://jsonplaceholder.typicode.com/users'
response = requests.get(url)

# Check if request was successful
if response.status_code == 200:
    users_data = response.json()

    # Extract relevant field
    users_list = []
    for user in users_data:
        users_list.append({
            'id': user['id'],
            'name': user['name'],
            'username': user['username'],
            'email': user['email'],
            'city': user['address']['city'] 
        })
    df = pd.DataFrame(users_list)
    print(df)
    print(f"\nFetched {len(df)} users successfully")
else:
    print(f"Error: {response.status_code}")