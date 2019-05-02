import pandas as pd
import boto3
from io import StringIO
import os

# Define global config S3  output
S3_BUCKET = 'st-batch-demo'
S3_DATA_PATH = 'airflow/today/data/' 
ROOM_ENG_FILE = "user_to_room_engagements.csv"
CLS_ROOM_FILE = "room_closest_connections.csv"

# Define Runtime config
N_CLOSEST = 10

# define some helper functions

def agg_closest(x):
    return ','.join([str(user) for user in x.head(N_CLOSEST).values])

def agg_weight(x):
    return ','.join(['{:.2f}'.format(w) for w in x.head(N_CLOSEST).values])

def write_to_s3(content, filename, bucket, path):
    # define S3 connection in function call to avoid timeout
    csv_buffer = StringIO()
    content.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, os.path.join(path, filename)).put(Body=csv_buffer.getvalue())

def read_from_s3(filename, bucket, path):
    # define S3 connection in function call to avoid timeout
    s3_client = boto3.client('s3')
    key = os.path.join(path, filename)
    csv_obj = s3_client.get_object(Bucket=bucket, Key=key)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_string))
    return df

# get closest user engagements from s3
room_engagements = read_from_s3(ROOM_ENG_FILE, S3_BUCKET, S3_DATA_PATH)

# now we want to count the number of room engagements for each pair to get a weight and take the top N
room_eng_count = room_engagements.groupby(["user_id", "engaged_with_room_id"]).agg({'weight': sum}).reset_index()
# now we want to take the N_CLOSEST engaged with users and their corresponding weights
room_eng_count.sort_values(['weight', 'engaged_with_room_id'], ascending=[False, True], inplace=True)
closest_rooms = room_eng_count.groupby('user_id').agg({'engaged_with_room_id': agg_closest, 'weight': agg_weight}).reset_index()

write_to_s3(closest_rooms, CLS_ROOM_FILE, S3_BUCKET, S3_DATA_PATH)  