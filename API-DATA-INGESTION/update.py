# Update existing records with put

import requests 

def update_post(post_id, title, body, user_id):
    url = f'https://jsonplaceholder.typicode.com/posts/{post_id}'
    
    data = {
        'id': post_id,
        'title': title,
        'body': body,
        'userId': user_id
    }

    response = requests.put(url, json=data)

    if response.status_code == 200:
        print("Post updated successfully")
        return response.json()
    
    else:
        print(f"Error: {response.status_code}")
        return None

updated_post = update_post(1, "Updated Title", "Updated Body", 1)
print(updated_post)

# Partial update using patch

def partial_update_post(post_id, **kwargs):
    url = f'https://jsonplaceholder.typicode.com/posts/{post_id}'

    response = requests.patch(url, json=kwargs)

    if response.status_code == 200:
        print("Post updated successfully")
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        return None
    

partial_updated = partial_update_post(1, title="Only Title Updated")
print(partial_updated)
