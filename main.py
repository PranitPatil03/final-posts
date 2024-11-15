import praw
import json
import uuid
from datetime import datetime, timezone
import logging
import re
import os
from pymongo import MongoClient
from dotenv import load_dotenv
import openai

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

# OpenAI API configuration
openai.api_key = os.getenv("OPENAI_API_KEY")

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

reddit = praw.Reddit(client_id=REDDIT_CLIENT_ID, client_secret=REDDIT_CLIENT_SECRET, user_agent=REDDIT_USER_AGENT)

POST_LIMIT_PER_SUBREDDIT = 2
TOTAL_THRESHOLD = 30

OUTPUT_SCRAPED_FILE = "./data/scraped_posts.json"
OUTPUT_FILTERED_FILE = "./data/filtered_posts.json"
OUTPUT_PROCESSED_FILE = "./data/processed_posts.json"

# Keywords for classification
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

# Helper Function: Classify posts by topics based on context
def classify_post(title, content):
    context = title + " " + content
    for category, keywords in TOPICS_KEYWORDS.items():
        if any(keyword.lower() in context.lower() for keyword in keywords):
            if len(content.split()) > 30:  # Ensure content has meaningful context
                return category
    return None

# Fetch top 5 comments for a submission
def fetch_comments(submission, max_comments=5):
    comments = []
    try:
        submission.comments.replace_more(limit=0)  # Load all comments
        for comment in submission.comments[:max_comments]:  # Fetch top `max_comments`
            comments.append({
                "author": str(comment.author) if comment.author else "Anonymous",
                "body": comment.body,
                "upvotes": comment.score,
                "created_utc": datetime.fromtimestamp(comment.created_utc, timezone.utc).isoformat(),
            })
    except Exception as e:
        logging.error(f"Error fetching comments: {e}")
    return comments

# Step 1: Scrape Posts
def scrape_posts():
    results = []
    for subreddit in SUBREDDITS:
        logging.info(f"Fetching posts from subreddit: {subreddit}")
        try:
            for submission in reddit.subreddit(subreddit).new(limit=POST_LIMIT_PER_SUBREDDIT):
                if len(results) >= TOTAL_THRESHOLD:
                    logging.info("Reached total threshold. Stopping.")
                    return results

                title = submission.title
                content = submission.selftext or ""
                category = classify_post(title, content)

                if category:
                    post = {
                        "id": submission.id,
                        "title": title,
                        "content": content,
                        "subreddit": subreddit,
                        "author": str(submission.author) if submission.author else "Anonymous",
                        "upvotes": submission.score,
                        "category": category,
                        "created_utc": datetime.fromtimestamp(submission.created_utc, timezone.utc).isoformat(),
                        "url": submission.url,
                        "comments": fetch_comments(submission),  # Add top 5 comments
                    }
                    results.append(post)
                    logging.info(f"Scraped post ID: {submission.id}")
        except Exception as e:
            logging.error(f"Error fetching subreddit {subreddit}: {e}")
    return results

# Step 2: Remove Duplicate Titles
def remove_duplicate_titles(posts):
    unique_titles = {}
    filtered_posts = []

    for post in posts:
        if post["title"] not in unique_titles:
            unique_titles[post["title"]] = True
            filtered_posts.append(post)
        else:
            logging.info(f"Duplicate removed: {post['title']}")
    return filtered_posts

# Step 3: Clean and Improve Content
def clean_content(content):
    clean_text = re.sub(r"<[^>]*>", "", content)  # Remove HTML tags
    clean_text = re.sub(r"\[.*?\]\(.*?\)", "", clean_text)  # Remove markdown links
    clean_text = re.sub(r"\*|\_", "", clean_text)  # Remove markdown emphasis
    clean_text = re.sub(r"\s+", " ", clean_text).strip()  # Normalize whitespace
    return clean_text

def paraphrase_content(content):
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are an assistant that paraphrases text concisely while preserving its original meaning and avoiding length increase."
                },
                {
                    "role": "user",
                    "content": f"Paraphrase the following content without changing its meaning or increasing its length:\n\n{content}"
                }
            ],
            max_tokens=500,
            temperature=0.7,
        )
        return response["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logging.error(f"Paraphrasing failed: {e}")
        return content

def process_content(posts):
    for post in posts:
        post["content"] = paraphrase_content(clean_content(post["content"]))
    return posts

# Step 4: Save to File (Updated to Serialize for JSON)
def save_to_file(data, file_path):
    """
    Save the data to a JSON file after serializing non-serializable fields like ObjectId.
    """
    try:
        serialized_data = serialize_for_json(data)  # Serialize before saving
        with open(file_path, "w") as file:
            json.dump(serialized_data, file, indent=4)
        logging.info(f"Data saved to {file_path}")
    except Exception as e:
        logging.error(f"Failed to save data to file {file_path}: {e}")

# Step 5: Upload Data to MongoDB
def upload_to_mongodb(data, collection_name):
    """
    Uploads data to the specified MongoDB collection.
    """
    try:
        collection = db[collection_name]
        if isinstance(data, list):
            data = serialize_for_json(data)  # Serialize data before uploading
            collection.insert_many(data)  # Insert all data records
            logging.info(f"Uploaded {len(data)} records to MongoDB collection '{collection_name}'.")
        else:
            logging.warning(f"Data format not supported for MongoDB upload.")
    except Exception as e:
        logging.error(f"Failed to upload data to MongoDB collection '{collection_name}': {e}")

# Helper: Serialize MongoDB Records for JSON
def serialize_for_json(data):
    """
    Converts ObjectId and other non-serializable types to strings for JSON serialization.
    """
    if isinstance(data, list):
        return [
            {**record, "_id": str(record["_id"])} if "_id" in record else record
            for record in data
        ]
    elif isinstance(data, dict):
        return {**data, "_id": str(data["_id"])} if "_id" in data else data
    return data

# Main Script Execution with MongoDB Upload and Serialization
def main():
    try:
        logging.info("Starting Reddit scraper...")

        # Step 1: Scrape
        scraped_posts = scrape_posts()
        save_to_file(scraped_posts, OUTPUT_SCRAPED_FILE)
        upload_to_mongodb(scraped_posts, "scraped_posts")  # Upload scraped posts

        # Step 2: Remove Duplicates
        filtered_posts = remove_duplicate_titles(scraped_posts)
        save_to_file(filtered_posts, OUTPUT_FILTERED_FILE)
        upload_to_mongodb(filtered_posts, "filtered_posts")  # Upload filtered posts

        # Step 3: Clean and Improve Content
        processed_posts = process_content(filtered_posts)
        save_to_file(processed_posts, OUTPUT_PROCESSED_FILE)
        upload_to_mongodb(processed_posts, "processed_posts")  # Upload processed posts

        logging.info(f"Completed processing {len(processed_posts)} posts and uploaded to MongoDB.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
