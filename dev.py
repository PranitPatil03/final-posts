import praw
import json
from datetime import datetime, timezone
import logging
import time
from pymongo import MongoClient

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Reddit API credentials (replace with your own)
CLIENT_ID = 'VMEp44xTVtl0lgUGbjSBcQ'
CLIENT_SECRET = 'wMRISmcP_vxrtXMLqFFW-Mpr0n0uWg'
USER_AGENT = 'RedditScraper:v1.0 (by /u/pranit3112)'

# MongoDB connection (update with your details)
MONGO_URI = "mongodb+srv://patilpranit3112:XTw9MyxYWov9Z1aa@cluster0.ffbjy.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "reddit_scraper"
COLLECTION_NAME = "posts"

# Initialize MongoDB client
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Initialize Reddit API client
reddit = praw.Reddit(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, user_agent=USER_AGENT)

# Search parameters
SUBREDDITS = [
    "logistics",
    "shipping",
    "supplychain",
    "freight",
    "transportation",
    "operations",
    "packaging",
    "warehousing",
    "3pl",
    "supplychainlogistics",
    "logisticsmanagement",
    "supplychainmanagement",
    "trucking",
    "airfreight",
    "maritimelogistics",
    "lastmiledelivery",
    "inventorymanagement",
    "sustainablelogistics",
    "freightbrokers",
    "logisticstechnology"
]

TOPICS_KEYWORDS = {
    "Parcel Shipping": ["parcel shipping", "package shipping", "parcel delivery", "package transit"],
    "Sustainable Packaging": ["eco-friendly packaging", "sustainable packaging", "green packaging"],
    "Last Mile Innovation": ["last mile delivery", "last mile solutions", "final delivery stage"],
    "Integration": ["system integration", "platform integration", "integration with"],
    "Carrier Solutions": ["carrier options", "carrier comparison", "shipping carriers", "freight carriers"],
    "Eco-Friendly": ["eco-friendly", "environmentally friendly", "sustainable", "green"],
    "3-2-1 Shipping": ["3-2-1 shipping", "3-2-1 logistics"],
    "Just-In-Time Inventory": ["just-in-time inventory", "JIT inventory", "inventory management"],
    "Cross-Docking": ["cross-docking", "dock transfer", "direct unloading"],
    "Distributed Inventory": ["distributed inventory", "inventory distribution", "regional inventory"],
    "Last-Mile Delivery Solutions": ["last-mile solutions", "last-mile logistics", "final mile delivery"],
    "Freight Consolidation": ["freight consolidation", "shipment consolidation", "consolidated freight"],
    "Dynamic Routing": ["dynamic routing", "adaptive routing", "route optimization"],
    "Third-Party Logistics (3PL)": ["third-party logistics", "3PL", "outsourced logistics"],
    "Seasonal Planning": ["seasonal planning", "holiday planning", "peak season planning"],
    "Cycle Counting": ["cycle counting", "inventory counting", "inventory auditing"],
    "Sales and Operations Planning (S&OP)": ["sales and operations planning", "S&OP", "sales planning"],
    "Cost-to-Serve Analysis": ["cost-to-serve", "cost analysis", "serve cost analysis"],
}

OUTPUT_FILE = "processed_posts_1.json"
TOTAL_THRESHOLD = 3500
PER_SUBREDDIT_LIMIT = 350

# Helper function to classify posts based on keywords
def classify_post(title, content):
    for category, keywords in TOPICS_KEYWORDS.items():
        for keyword in keywords:
            if keyword.lower() in title.lower() or keyword.lower() in content.lower():
                return category
    return None

# Function to fetch comments for a post
def fetch_comments(submission, max_comments=10):
    comments = []
    submission.comments.replace_more(limit=0)  # Expand all "more comments"
    for comment in submission.comments[:max_comments]:  # Limit number of comments
        comments.append({
            "id": comment.id,
            "author": str(comment.author) if comment.author else "Anonymous",
            "body": comment.body,
            "upvotes": comment.score,
            "created_utc": datetime.fromtimestamp(comment.created_utc, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
        })
    return comments

# Function to fetch posts from a single subreddit
def fetch_posts_from_subreddit(subreddit, per_subreddit_limit, max_comments, results):
    fetched_posts = 0
    try:
        for submission in reddit.subreddit(subreddit).new(limit=None):
            if len(results) >= TOTAL_THRESHOLD or fetched_posts >= per_subreddit_limit:
                break

            post_category = classify_post(submission.title, submission.selftext)
            if post_category:
                post_data = {
                    "id": submission.id,
                    "title": submission.title,
                    "content": submission.selftext,
                    "subreddit": subreddit,
                    "type": "question" if submission.is_self else "discussion",
                    "author": str(submission.author) if submission.author else "Anonymous",
                    "upvotes": submission.score,
                    "category": post_category,
                    "created_utc": datetime.fromtimestamp(submission.created_utc, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                    "url": submission.url,
                    "comments": fetch_comments(submission, max_comments),  # Include comments
                }
                results.append(post_data)
                fetched_posts += 1
                logging.info(f"Scraped post ID: {submission.id}")

    except Exception as e:
        logging.error(f"Error fetching posts from {subreddit}: {e}")

    logging.info(f"Fetched {fetched_posts} posts from subreddit: {subreddit}")

# Main function to scrape posts iteratively
def scrape_all_posts(max_comments=10):
    results = []
    while len(results) < TOTAL_THRESHOLD:
        logging.info(f"Starting new round of scraping. Current total posts: {len(results)}")
        for subreddit in SUBREDDITS:
            if len(results) >= TOTAL_THRESHOLD:
                break
            fetch_posts_from_subreddit(subreddit, PER_SUBREDDIT_LIMIT, max_comments, results)
    return results

# Function to save data to MongoDB
def save_to_mongodb(data):
    try:
        if data:
            collection.insert_many(data)
            logging.info(f"Saved {len(data)} posts to MongoDB collection '{COLLECTION_NAME}'")
        else:
            logging.warning("No data to save to MongoDB.")
    except Exception as e:
        logging.error(f"Error saving to MongoDB: {e}")

# Main entry point
def main():
    try:
        logging.info("Starting Reddit scraper...")
        posts = scrape_all_posts(max_comments=5)  # Limit comments to 5 per post
        logging.info(f"Scraped {len(posts)} relevant posts.")
        
        # Save results to JSON file
        with open(OUTPUT_FILE, 'w') as file:
            json.dump(posts, file, indent=4)
        
        logging.info(f"Data saved to {OUTPUT_FILE}")
        
        # Commenting out MongoDB saving for testing
        # save_to_mongodb(posts)  # This line is now commented out
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
