# Task: Fetch posts from a specific user using query parameters.
import requests
import pandas as pd

url = 'https://jsonplaceholder.typicode.com/posts'

# Method 1: Parameters in url
# response = requests.get(f"{url}?userId=1")

# Method 2: Using params parameter (preferred)
params = {'userId': 1}
response = requests.get(url, params=params)

if response.status_code == 200:
    posts = response.json()
    df = pd.DataFrame(posts)
    print(f"User 1 has {len(df)} posts.")
    print(df[['id', 'title']].head())
else:
    print(f"Error: { response.status_code }")


# Fetch posts from multiple users
user_ids = [1,2,3]
all_posts = []

for user_id in user_ids:
    params = {'userId': user_id}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        posts = response.json()
        all_posts.extend(posts)

df_all = pd.DataFrame(all_posts)
print(f"\nTotal posts from users 1-3: {len(df_all)}")
