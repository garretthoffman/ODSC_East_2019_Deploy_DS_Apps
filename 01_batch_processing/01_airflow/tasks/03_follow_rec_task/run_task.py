import pandas as pd
import numpy as np
import itertools as it
from io import StringIO
import boto3
import os

# Define date ranges for analysis, we won't actually use this but this would be set to limit the engagements
# that we are analyzing
LAST_DATE = "yesterday"
CURRENT_TIME = "today"

# Define global config S3  output
S3_BUCKET = 'st-batch-demo'
S3_DATA_PATH = 'airflow/today/data/' 
CLS_CNC_FILE = "user_closest_connections.csv"
FOLLOWS_FILE = "follows.csv"
S3_OUTPUT_PATH = "airflow/today/user"
RES_FILE_NAME = "follow_recs.csv"

# Define Runtime config
N_CLOSEST = 10
N_REC = 5

# define some helper functions
def agg_closest(x):
    return ','.join([str(user) for user in x.head(N_CLOSEST).values])

def agg_reqs(x):
    return ','.join([str(user) for user in x.head(N_REC).values])

def agg_reqs_weight(x):
    return ','.join(['{:.2f}'.format(w) for w in x.head(N_REC).values])

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

# get closest connections and follows from S3
closest_connections = read_from_s3(CLS_CNC_FILE, S3_BUCKET, S3_DATA_PATH)
follows = read_from_s3(FOLLOWS_FILE, S3_BUCKET, S3_DATA_PATH)

# we need to unstack the closest connections into unique records of "first degree connections"
closest_connections.loc[:, "engaged_with_user_id"] = closest_connections.loc[:, "engaged_with_user_id"].apply(lambda x: [int(u) for u in x.split(",")])
closest_connections.loc[:, "weight"] = closest_connections.loc[:, "weight"].apply(lambda x: [float(w) for w in x.split(",")])
first_degree_cnx = pd.DataFrame({
            "user_id": np.repeat(closest_connections['user_id'].values, closest_connections['engaged_with_user_id'].str.len()),
            "engaged_with_user_id": list(it.chain.from_iterable(closest_connections['engaged_with_user_id'])),
            "weight": list(it.chain.from_iterable(closest_connections['weight']))
        })

# now we merge first degree connections itself (engaged with user id to user id) to get second degree cnx
second_degree_cnx = first_degree_cnx.merge(first_degree_cnx, 
                                           left_on="engaged_with_user_id",
                                           right_on="user_id",
                                           suffixes=("", "_sec_deg"))
# drop copy of engaged_with_user_id (user_id_sec_deg) and first degree weight
second_degree_cnx.drop(["user_id_sec_deg", "weight"], inplace=True, axis=1)

# remove 2nd degree that you already follow
temp = pd.merge(second_degree_cnx, follows, 
                left_on=["user_id", "engaged_with_user_id_sec_deg"],
                right_on=["user_id", "engaged_with_user_id"],
                how="left", 
                indicator=True, 
                suffixes=("", "_follow"))

second_degree_cnx_valid = temp[temp["_merge"] == "left_only"].copy()
# drop unnecesary fields
second_degree_cnx_valid.drop(["_merge", "engaged_with_user_id_follow"], inplace=True, axis=1)
# drop if the second degree connection is the user
identity_mask = second_degree_cnx_valid["user_id"] != second_degree_cnx_valid["engaged_with_user_id_sec_deg"]
second_degree_cnx_valid = second_degree_cnx_valid[identity_mask].copy()

# now we want to aggregate by second degree connection engagement, we will also collect the intermediate connections
# that lead to this rec
second_degree_eng_count = second_degree_cnx_valid.groupby(["user_id", "engaged_with_user_id_sec_deg"]).agg({"weight_sec_deg": sum, "engaged_with_user_id": agg_closest}).reset_index()

# sort agg weights and group by user_id to get recs
second_degree_eng_count.sort_values(["weight_sec_deg", "user_id"], ascending=[False, True], inplace=True)
recs = second_degree_eng_count.groupby("user_id").agg({"engaged_with_user_id_sec_deg": agg_reqs,
                                                       "weight_sec_deg": agg_reqs_weight,
                                                       "engaged_with_user_id": lambda x: ";".join(tuple(x)[:N_REC])}).reset_index()

# rename colums for recs         
recs.rename(columns={"engaged_with_user_id_sec_deg": "recommendations",
                     "weight_sec_deg": "recommendation_weights",
                     "engaged_with_user_id": "recommendation_reasons"},
            inplace=True)

write_to_s3(recs, RES_FILE_NAME, S3_BUCKET, S3_OUTPUT_PATH)