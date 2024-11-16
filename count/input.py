import json
from collections import Counter

OUTPUT_FILE = "./data/scraped_posts.json"

def count_scraped_data(file_path):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            print("JSON file loaded successfully!")
        
        # Total number of posts
        total_posts = len(data)
        
        # Count posts by subreddit
        subreddit_counts = Counter(post['subreddit'] for post in data)
        
        # Count posts by category
        category_counts = Counter(post['category'] for post in data if 'category' in post)
        
        # Total comments and upvotes
        total_comments = sum(len(post.get('comments', [])) for post in data)
        total_post_upvotes = sum(post.get('upvotes', 0) for post in data)
        total_comment_upvotes = sum(
            sum(comment.get('upvotes', 0) for comment in post.get('comments', []))
            for post in data
        )
        
        # Most active authors
        post_authors = Counter(post['author'] for post in data if 'author' in post)
        comment_authors = Counter(
            comment['author'] for post in data for comment in post.get('comments', [])
        )
        most_active_post_authors = post_authors.most_common(5)
        most_active_comment_authors = comment_authors.most_common(5)
        
        # Display results
        print(f"Total Posts Scraped: {total_posts}")
        print(f"Total Comments Scraped: {total_comments}")
        print(f"Total Post Upvotes: {total_post_upvotes}")
        print(f"Total Comment Upvotes: {total_comment_upvotes}")
        
        print("\nPosts by Subreddit:")
        for subreddit, count in subreddit_counts.items():
            print(f"  {subreddit}: {count}")
        
        print("\nPosts by Category:")
        for category, count in category_counts.items():
            print(f"  {category}: {count}")
        
        print("\nMost Active Post Authors:")
        for author, count in most_active_post_authors:
            print(f"  {author}: {count} posts")
        
        print("\nMost Active Comment Authors:")
        for author, count in most_active_comment_authors:
            print(f"  {author}: {count} comments")
        
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in file '{file_path}'. Details: {e}")

# Run the function
if __name__ == "__main__":
    count_scraped_data(OUTPUT_FILE)
