# Task: send data to an api using POST request

import requests
import json
import time

def create_post(title, body, user_id):
    url = 'https://jsonplaceholder.typicode.com/posts'

    data = {
        'title': title,
        'body': body,
        'userId': user_id
    }

    # Method 1: Send data as JSON
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, json=data, headers=headers)

    # Method 2: Send as data (form-encoded)
    # response = requests.post(url, data=data)

    if response.status_code == 201:
        print("✓ Post created successfully")
        return response.json()
    else:
        print(f"✗ Error: {response.status_code}")
        print(f"Response: {response.text}")
        return None
    
new_post = create_post(
    title="My API Test",
    body="This is a test post created via API",
    user_id=1
)

if new_post:
    print(f"Created post with ID: {new_post['id']}")
    print(f"Title: {new_post['title']}")

# Bulk create multiple post
def bulk_create_post(posts_data):
    url = 'https://jsonplaceholder.typicode.com/posts'

    created_posts = []
    failed_posts = []

    for i, post_data in enumerate(posts_data):
        print(f"Creating post {i}/{len(posts_data)}...")

        try:
            response = requests.post(url, json=post_data, timeout=10)

            if response.status_code == 201:
                created_posts.append(response.json())
                print(f" ✓ Success")
            else:
                failed_posts.append({
                    'data': post_data,
                    'status_code': response.status_code,
                    'error': response.text
                })
                print(f"  ✗ Failed: {response.status_code}")
        
        except Exception as e:
            failed_posts.append({
                'data': post_data,
                'error': str(e)
            })
            print(f"  ✗ Exception: {str(e)}")

        time.sleep(0.5)
    return created_posts, failed_posts

posts_to_create = [
    {'title': 'Post 1', 'body': 'Content 1', 'userId': 1},
    {'title': 'Post 2', 'body': 'Content 2', 'userId': 1},
    {'title': 'Post 3', 'body': 'Content 3', 'userId': 2},
]

created, failed = bulk_create_post(posts_to_create)
print(f"\nSummary:")
print(f"Created: {len(created)}")
print(f"Failed: {len(failed)}")

if failed:
    print("\nFailed posts:")
    for fail in failed:
        print(f"  - {fail}")
