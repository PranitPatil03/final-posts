import praw
import json
import logging
from datetime import datetime, timezone
from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

# MongoDB Configuration
MONGO_URI = "mongodb+srv://patilpranit3112:XTw9MyxYWov9Z1aa@cluster0.ffbjy.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "reddit_scraper"
COLLECTION_NAME = "posts"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Reddit API Configuration
REDDIT_CLIENT_ID = 'VMEp44xTVtl0lgUGbjSBcQ'
REDDIT_CLIENT_SECRET = 'wMRISmcP_vxrtXMLqFFW-Mpr0n0uWg'
REDDIT_USER_AGENT = 'RedditScraper:v1.0 (by /u/pranit3112)'
reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_CLIENT_SECRET,
    user_agent=REDDIT_USER_AGENT
)

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

POST_LIMIT_PER_SUBREDDIT = 2000  # Set this to your desired limit
OUTPUT_SCRAPED_FILE = "./data/scraped_posts.json"


# Helper Function: Fetch Top Comments
def fetch_comments(submission, max_comments=5):
    comments = []
    try:
        submission.comments.replace_more(limit=0)
        for comment in submission.comments[:max_comments]:
            comments.append({
                "author": str(comment.author) if comment.author else "Anonymous",
                "body": comment.body,
                "upvotes": comment.score,
                "created_utc": datetime.fromtimestamp(comment.created_utc, timezone.utc).isoformat(),
            })
    except Exception as e:
        logging.error(f"Error fetching comments: {e}")
    return comments


# Helper Function: Scrape Subreddit
def scrape_subreddit(subreddit, limit=2000):
    results = []
    after = None
    fetched = 0

    while fetched < limit:
        try:
            posts = reddit.subreddit(subreddit).new(limit=100, params={"after": after})
            for submission in posts:
                if fetched >= limit:
                    break
                post = {
                    "id": submission.id,
                    "title": submission.title,
                    "content": submission.selftext or "",
                    "subreddit": subreddit,
                    "author": str(submission.author) if submission.author else "Anonymous",
                    "upvotes": submission.score,
                    "created_utc": datetime.fromtimestamp(submission.created_utc, timezone.utc).isoformat(),
                    "url": submission.url,
                    "comments": fetch_comments(submission),
                }
                results.append(post)
                fetched += 1
                logging.info(f"Scraped post ID: {submission.id}")
            
            # Update `after` for pagination
            after = results[-1]["id"] if results else None
            if not after:
                break
        except Exception as e:
            logging.error(f"Error fetching posts from subreddit {subreddit}: {e}")
            break

    return results


# Step 1: Scrape All Subreddits
def scrape_all_subreddits():
    all_results = []
    for subreddit in SUBREDDITS:
        logging.info(f"Scraping subreddit: {subreddit}")
        subreddit_results = scrape_subreddit(subreddit, limit=POST_LIMIT_PER_SUBREDDIT)
        all_results.extend(subreddit_results)
    return all_results


# Step 2: Save to File
def save_to_file(data, file_path):
    try:
        with open(file_path, "w") as file:
            json.dump(data, file, indent=4)
        logging.info(f"Data saved to {file_path}")
    except Exception as e:
        logging.error(f"Failed to save data to file {file_path}: {e}")


# Step 3: Upload Data to MongoDB
def upload_to_mongodb(data, collection_name):
    try:
        collection = db[collection_name]
        if isinstance(data, list):
            collection.insert_many(data)
            logging.info(f"Uploaded {len(data)} records to MongoDB collection '{collection_name}'.")
        else:
            logging.warning("Data format not supported for MongoDB upload.")
    except Exception as e:
        logging.error(f"Failed to upload data to MongoDB collection '{collection_name}': {e}")


# Main Execution
def main():
    logging.info("Starting Reddit scraper...")

    # Scrape Subreddits
    scraped_posts = scrape_all_subreddits()
    save_to_file(scraped_posts, OUTPUT_SCRAPED_FILE)
    upload_to_mongodb(scraped_posts, COLLECTION_NAME)

    logging.info("Scraping completed.")


if __name__ == "__main__":
    main()
