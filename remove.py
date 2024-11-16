import json

# Load JSON data from file
with open('./data/cleaned_posts.json', 'r') as file:
    data = json.load(file)

# Function to remove "_id" and "id" fields from posts
def remove_id_fields(posts):
    for post in posts:
        post.pop('_id', None)  # Remove '_id' field if it exists
        post.pop('id', None)   # Remove 'id' field if it exists
    return posts

# Remove the fields
cleaned_data = remove_id_fields(data)

# Save the cleaned data back to a JSON file
with open('cleaned_posts.json', 'w') as file:
    json.dump(cleaned_data, file, indent=4)

print("Fields '_id' and 'id' removed. Cleaned data saved to 'cleaned_posts.json'.")
