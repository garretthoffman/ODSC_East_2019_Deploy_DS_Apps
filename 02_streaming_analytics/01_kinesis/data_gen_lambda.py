import time
import datetime as dt
import numpy as np
import psycopg2
import boto3
import json
import os

def generate_sample_data(event, context):

    # Open kinesis client and define stream name
    client = boto3.client('kinesis')
    STREAM_NAME = "stream_demo"

    # Grab config
    HOST = os.environ.get('DB_HOST') or "127.0.0.1"
    DB_NAME = os.environ.get('DB_NAME') or "st_lite"
    USER = os.environ.get('DB_USER') or "GarrettHoffman"
    PASSWORD = os.environ.get('DB_PASSWORD') or ""

    # define our universe of symbols
    SYMBOLS = ["FB", "AMZN", "AAPL", "NFLX", "GOOG"]

    # define helper function to generate our data
    def generate_symbol():
        symbol = np.random.choice(SYMBOLS)
        return symbol

    def generate_sentiment_score():
        sentiment = 2 * np.random.random() - 1
        return round(sentiment, 5)

    # connect to db
    # now we will create the postgres tables and insert rows from the lists we created
    # open our connection
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

    # create our raw sentiment table
    try: 
        cur.execute("CREATE TABLE IF NOT EXISTS raw_sent_stream (created_at timestamp, message_id int, symbol varchar, sent_score numeric) ;")
    except psycopg2.Error as e: 
        print("Error: Issue creating users table")
        print (e)

    # generate data for 10 minutes and write it to postgress
    t0 = dt.datetime.utcnow()
    t1 = dt.datetime.utcnow()
    seconds_elapsed =  (t1 - t0).total_seconds()
    message_id = 0
    while seconds_elapsed < 600:
        created_at = dt.datetime.utcnow()
        message_id += 1
        symbol = generate_symbol()
        sent_score = generate_sentiment_score()

        print("Created At:", created_at, "Message Id:", message_id, "Symbol:", symbol, "Sentiment Score:", sent_score)
        try: 
            cur.execute("INSERT INTO raw_sent_mini_batch (created_at, message_id, symbol, sent_score) VALUES (%s, %s, %s, %s)", \
                        (created_at, message_id, symbol, sent_score))
        except psycopg2.Error as e: 
            print("Error: Inserting Row")
            print (e)

        # send record to kinesis
        stream_data = {
            "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"), 
            "message_id" : message_id,
            "symbol": symbol, 
            "sent_score": sent_score
        }
        r = client.put_record(StreamName=STREAM_NAME, Data=json.dumps(stream_data), PartitionKey="partitionkey")
        
        # sleep for a millisecond
        time.sleep(0.05)

        # update seconds elapsed
        t1 = dt.datetime.utcnow()
        seconds_elapsed =  (t1 - t0).total_seconds()

if __name__ == "__main__":
    generate_sample_data("", "")  

