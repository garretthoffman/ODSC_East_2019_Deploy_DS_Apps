import psycopg2
import pandas as pd
import boto3
from io import StringIO
import os

# Define psql config
HOST = os.environ.get('DB_HOST') or "st-deploy-ds-apps-db.cypzti2esilk.us-east-1.rds.amazonaws.com"
DB_NAME = os.environ.get('DB_NAME') or "stdemo"
USER = os.environ.get('DB_USER') or "odsc"
PASSWORD = os.environ.get('DB_PASSWORD') or "password"

# Define date ranges for analysis, we won't actually use this but this would be set to limit the engagements
# that we are analyzing
LAST_DATE = "yesterday"
CURRENT_TIME = "today"

# Define global config S3  output
S3_BUCKET = 'st-batch-demo'
S3_DATA_PATH = 'airflow/today/data/' 
USER_ENG_FILE = "user_to_user_engagements.csv"
FOLLOWS_FILE = "follows.csv"
ROOM_ENG_FILE = "user_to_room_engagements.csv"
SUBS_FILE = "subscriptions.csv"

# define some helper functions

def write_to_s3(content, filename, bucket, path):
    # define S3 connection in function call to avoid timeout
    csv_buffer = StringIO()
    content.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, os.path.join(path, filename)).put(Body=csv_buffer.getvalue())

# open psql connections
try: 
    conn = psycopg2.connect(f"host={HOST} dbname={DB_NAME} user={USER} password={PASSWORD}")
except psycopg2.Error as e: 
    print("Error: Could not make connection to the Postgres database")
    print(e)

# read in data from psql
messages = pd.read_sql_query("SELECT id, user_id, room_id, mention_ids FROM messages", conn, coerce_float=False)
likes = pd.read_sql_query("SELECT user_id, message_id FROM likes", conn, coerce_float=False)
follows = pd.read_sql_query("SELECT user_id, following_user_id FROM follows", conn, coerce_float=False)
subscriptions = pd.read_sql_query("SELECT user_id, room_id FROM subscriptions", conn, coerce_float=False)

# first we will get first degree user engagments to find who we are closest too
# we need to get all user > user engagements so we need to "unstack" the mention_ids
mentions = []
for message, user, room, mention_ids in messages.values:
    if not mentions:
        continue
    msg_mentions = [int(mention_id) for mention_id in mention_ids.split(",")]
    for mention in msg_mentions:
        mentions.append([user, mention])

mentions = pd.DataFrame(mentions, columns=["user_id", "mentioned_user_id"])

# we need to joing likes and messages to get the user that posted the message that was liked
liked_users = likes.merge(messages, how='left', left_on="message_id", right_on="id", suffixes=("", "_liked"))
liked_users = liked_users.loc[:, ("user_id", "user_id_liked")]

# merge all user to user engagements into a single tables
# first update all of the columne names to be uniform
follows.rename(columns={"following_user_id": "engaged_with_user_id"}, inplace=True)
mentions.rename(columns={"mentioned_user_id": "engaged_with_user_id"}, inplace=True)
liked_users.rename(columns={"user_id_liked": "engaged_with_user_id"}, inplace=True)

user_engagements = pd.concat([follows, mentions, liked_users], axis=0)
# add a weight of 1 per engagement
user_engagements.loc[:, 'weight'] = 1

write_to_s3(user_engagements, USER_ENG_FILE, S3_BUCKET, S3_DATA_PATH)
write_to_s3(follows, FOLLOWS_FILE, S3_BUCKET, S3_DATA_PATH)  

# now we need to calculate weights for first degree room engagements
# we need to just pull out the rooms that people have messaged in
messaged_in_rooms = messages.loc[:, ("user_id", "room_id")].copy()

# we need to joing likes and messages to get the room the message that was liked was posted in
liked_rooms = likes.merge(messages, how='left', left_on="message_id", right_on="id", suffixes=("", "_liked"))
liked_rooms = liked_rooms.loc[:, ("user_id", "room_id")]

room_engagements = pd.concat([subscriptions, messaged_in_rooms, liked_rooms], axis=0)
room_engagements.rename(columns={"room_id": "engaged_with_room_id"}, inplace=True)
# add a weight of 1 per engagement
room_engagements.loc[:, 'weight'] = 1

write_to_s3(room_engagements, ROOM_ENG_FILE, S3_BUCKET, S3_DATA_PATH)
write_to_s3(subscriptions, SUBS_FILE, S3_BUCKET, S3_DATA_PATH)
