import datetime as dt
import psycopg2
import pandas as pd
import os

# Define psql config
HOST = os.environ.get('DB_HOST') or "st-deploy-ds-apps-db.cypzti2esilk.us-east-1.rds.amazonaws.com"
DB_NAME = os.environ.get('DB_NAME') or "stdemo"
USER = os.environ.get('DB_USER') or "odsc"
PASSWORD = os.environ.get('DB_PASSWORD') or "password"

# Define current time window
NOW = dt.datetime.utcnow()
MIN_END = NOW - dt.timedelta(seconds=NOW.second, microseconds=NOW.microsecond)
MIN_START = MIN_END - dt.timedelta(minutes=1)

# define processing functions
def get_bull_score(sent_score):
    if sent_score > 0:
        return sent_score
    return 0

def get_bear_score(sent_score):
    if sent_score < 0:
        return sent_score
    return 0

# connect to db
try: 
    conn = psycopg2.connect(f"host={HOST} dbname={DB_NAME} user={USER} password={PASSWORD}")
except psycopg2.Error as e: 
    print("Error: Could not make connection to the Postgres database")
    print(e)

# get our cursor
try: 
    cur = conn.cursor()
except psycopg2.Error as e: 
    print("Error: Could not get curser to the Database")
    print(e)

# set connection autocommit to be true
conn.set_session(autocommit=True)

# read the data from the current window
q = f"""
    SELECT symbol, sent_score 
    FROM raw_sent_mini_batch
    WHERE created_at >= '{MIN_START}'
    AND created_at < '{MIN_END}'
    """

# get data from psql
data = pd.read_sql_query(q, conn)
data.loc[:, 'bullish_score'] = data.loc[:, "sent_score"].apply(get_bull_score)
data.loc[:, 'bearish_score'] = data.loc[:, "sent_score"].apply(get_bear_score)

agg_data = data.groupby('symbol').agg({"bullish_score": sum, "bearish_score": sum}).reset_index()
agg_data.loc[:, "period_ending"] = MIN_END

# write values back to postgres
# create our raw sentiment table
try: 
    cur.execute("CREATE TABLE IF NOT EXISTS agg_sent_mini_batch \
                (symbol varchar, bullish_score numeric, bearish_score numeric, period_ending timestamp) ;")
except psycopg2.Error as e: 
    print("Error: Issue creating users table")
    print (e)

for symbol, bullish_score, bearish_score, period_ending in agg_data.values:
    try: 
        cur.execute("INSERT INTO agg_sent_mini_batch (symbol, bullish_score, bearish_score, period_ending) \
                     VALUES (%s, %s, %s, %s)", \
                    (symbol, bullish_score, bearish_score, period_ending))
    except psycopg2.Error as e: 
        print("Error: Inserting Row")
        print (e)
