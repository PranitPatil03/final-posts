import json

# Load JSON data
with open('./data/432.json', 'r') as file:
    data = json.load(file)

# Function to remove duplicate posts by title
def remove_duplicates(posts):
    seen_titles = {}
    unique_posts = []

    for post in posts:
        title = post['title']
        if title not in seen_titles:
            seen_titles[title] = post
        else:
            # Keep the post with more upvotes or the earlier creation date
            existing_post = seen_titles[title]
            if post['upvotes'] > existing_post['upvotes'] or post['created_utc'] < existing_post['created_utc']:
                seen_titles[title] = post

    # Collect unique posts
    unique_posts = list(seen_titles.values())
    return unique_posts

# Remove duplicates
unique_data = remove_duplicates(data)

# Save the cleaned data back to a JSON file
with open('cleaned_posts.json', 'w') as file:
    json.dump(unique_data, file, indent=4)

print("Duplicates removed and data saved to 'cleaned_posts.json'.")
