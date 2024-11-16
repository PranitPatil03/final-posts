import json
from collections import Counter

OUTPUT_FILE = "./data/cleaned_posts.json"

def count_scraped_data(file_path):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        
        # Total number of posts
        total_posts = len(data)
        
        # Count posts by subreddit
        subreddit_counts = Counter(post.get('subreddit', 'Unknown') for post in data)
        
        # Count posts by category
        category_counts = Counter(post.get('category', 'Uncategorized') for post in data)
        
        # Count total comments
        total_comments = sum(len(post.get('comments', [])) for post in data)
        
        # Count comments per post
        comments_per_post = {post['id']: len(post.get('comments', [])) for post in data}

        # Display results
        print(f"Total Posts Scraped: {total_posts}")
        print(f"Total Comments: {total_comments}\n")
        
        print("Posts by Subreddit:")
        for subreddit, count in subreddit_counts.items():
            print(f"  {subreddit}: {count}")
        
        print("\nPosts by Category:")
        for category, count in category_counts.items():
            print(f"  {category}: {count}")
        
        
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
    except json.JSONDecodeError:
        print(f"Error: File '{file_path}' is not valid JSON.")

# Run the function
if __name__ == "__main__":
    count_scraped_data(OUTPUT_FILE)
