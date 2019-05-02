import pandas as pd
import numpy as np
import psycopg2
import os

def generate_sample_data(event, context):

    # Grab config
    HOST = os.environ.get('DB_HOST') or "st-deploy-ds-apps-db.cypzti2esilk.us-east-1.rds.amazonaws.com"
    DB_NAME = os.environ.get('DB_NAME') or "stdemo"
    USER = os.environ.get('DB_USER') or "odsc"
    PASSWORD = os.environ.get('DB_PASSWORD') or "password"

    # Need to create mock data for user and room recommendation tasks
    # Need to create User, Room, Message Objects and Follow, Subscribes, 
    # Posts In, Mention, and Like Relationshipts

    # simply create users, rooms and messages
    users = np.arange(1000) + 1
    rooms = np.arange(200) + 1
    messages = np.arange(20000) + 1

    # for each user first randomly select number of people they follow uniformly between 10 and 200, 
    # then randomly sample who they follow
    follow_dict = {}
    for user in users:
        n = int(np.random.uniform(10, 200))
        follows = np.random.choice(a=users, size=n, replace=False)
        follow_dict[user] = []
        for follow in follows:
            if follow == user:
                continue
            follow_dict[user].append(follow)

    # create a reverse dict of followers relationship from follows dict, this will come in handy later
    # when we need to sample from a users followers
    follower_dict = {}
    for user in follow_dict:
        for follows in follow_dict[user]:
            if user not in follower_dict:
                follower_dict[user] = [follows]
            else:
                follower_dict[user].append(follows)   

    # for each user first randomly select number of rooms they subscribe to uniformly between 5 and 15, 
    # then randomly sample which rooms they subscribe to
    subscribes_dict = {}
    for user in users:
        n = int(np.random.uniform(5, 15))
        subscribes = np.random.choice(a=rooms, size=n, replace=False)
        subscribes_dict[user] = subscribes.tolist()
        
    # create a reverse dict of subscribers relationship from subscribes dict, this will come in handy later
    # when we need to sample from a rooms subscribers
    subscribers_dict = {}
    for user in subscribes_dict:
        for room in subscribes_dict[user]:
            if room not in subscribers_dict:
                subscribers_dict[room] = [user]
            else:
                subscribers_dict[room].append(user)  

    # for each message sample a random room
    posts_in_dict = {}
    for message in messages:
        posts_in_dict[message] = np.random.choice(rooms)
        
    # for each message choose a random author from the subscribers of the room it is posted in
    author_dict = {}
    for message in messages:
        room = posts_in_dict[message]
        subscribers = subscribers_dict[room]
        author_dict[message] = np.random.choice(subscribers)

    # for each message sample how many users were tagged in it from a possion distribution with lambda = 1.5
    # limited by the total number of users that they follow. Then sample who these users are from the people
    # that the user who posted the message follows
    post_mention_dict = {}
    for message in messages:
        author = author_dict[message]
        follows = follow_dict[author]
        n = min(len(follows), np.random.poisson(lam=1.5))
        mentions = np.random.choice(a=follows, size=n, replace=False)
        post_mention_dict[message] = mentions.tolist()

    # for each message sample how many users liked it from a uniform distribution from 0 to 25 limited 
    # by the total number of users that follow the auther. Then sample who these users are from the people
    # that follow the user who posted the message.
    post_like_dict = {}
    for message in messages:
        author = author_dict[message]
        followers = follower_dict[author]
        n = int(min(len(followers), np.random.uniform(0, 25)))
        likes = np.random.choice(a=followers, size=n, replace=False)
        post_like_dict[message] = likes.tolist()

    # now that we have all of the objects and relationships defined we need to construct our tables.
    # first we will define these as lists and then we will simply iterate through our lists to instert the rows
    # into postgres

    follows = []
    i = 1
    for user in follow_dict:
        for follow in follow_dict[user]:
            follows.append([i, user, follow])
            i += 1

    likes = []
    i = 1
    for message in post_like_dict:
        for user in post_like_dict[message]:
            likes.append([i, user, message])
            i += 1

    subscriptions = []
    i = 1
    for room in subscribers_dict:
        for user in subscribers_dict[room]:
            subscriptions.append([i, user, room])
            i += 1
    
    message_data = []
    for message in messages:
        author = author_dict[message]
        room = posts_in_dict[message]
        mentions = post_mention_dict[message]
        mentions = ",".join(str(m) for m in mentions)
        message_data.append([message, author, room, mentions])

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

    # define and fill users table
    try: 
        cur.execute("CREATE TABLE IF NOT EXISTS users (id int) ;")
    except psycopg2.Error as e: 
        print("Error: Issue creating users table")
        print (e)

    for user_id in users:
        try: 
            cur.execute("INSERT INTO users (id) VALUES (%s)", [int(user_id)])
        except psycopg2.Error as e: 
            print("Error: Inserting Rows")
            print (e)

    # define and fill rooms table
    try: 
        cur.execute("CREATE TABLE IF NOT EXISTS rooms (id int) ;")
    except psycopg2.Error as e: 
        print("Error: Issue creating rooms table")
        print (e)

    for room_id in rooms:
        try: 
            cur.execute("INSERT INTO rooms (id) VALUES (%s)", [int(room_id)])
        except psycopg2.Error as e: 
            print("Error: Inserting Rows")
            print (e)
    
    # define and fill follows table
    try: 
        cur.execute("CREATE TABLE IF NOT EXISTS follows (id int, user_id int, following_user_id int) ;")
    except psycopg2.Error as e: 
        print("Error: Issue creating follows table")
        print (e)

    for i, user_id, following_user_id in follows:
        try: 
            cur.execute("INSERT INTO follows (id, user_id, following_user_id) VALUES (%s, %s, %s)", \
                        (i, int(user_id), int(following_user_id)))
        except psycopg2.Error as e: 
            print("Error: Inserting Rows")
            print (e)

    # define and fill likes table
    try: 
        cur.execute("CREATE TABLE IF NOT EXISTS likes (id int, user_id int, message_id int) ;")
    except psycopg2.Error as e: 
        print("Error: Issue creating likes table")
        print (e)

    for i, user_id, message_id in likes:
        try: 
            cur.execute("INSERT INTO likes (id, user_id, message_id) VALUES (%s, %s, %s)", \
                        (i, int(user_id), int(message_id)))
        except psycopg2.Error as e: 
            print("Error: Inserting Rows")
            print (e)

    # define and fill subscriptions table
    try: 
        cur.execute("CREATE TABLE IF NOT EXISTS subscriptions (id int, user_id int, room_id int) ;")
    except psycopg2.Error as e: 
        print("Error: Issue creating subscriptions table")
        print (e)

    for i, user_id, room_id in subscriptions:
        try: 
            cur.execute("INSERT INTO subscriptions (id, user_id, room_id) VALUES (%s, %s, %s)", \
                        (i, int(user_id), int(room_id)))
        except psycopg2.Error as e: 
            print("Error: Inserting Rows")
            print (e)

    # define and fill messages table
    try: 
        cur.execute("CREATE TABLE IF NOT EXISTS messages (id int, user_id int, room_id int, mention_ids varchar) ;")
    except psycopg2.Error as e: 
        print("Error: Issue creating messages table")
        print (e)

    for message_id, user_id, room_id, mention_ids in message_data:
        try: 
            cur.execute("INSERT INTO messages (id, user_id, room_id, mention_ids) VALUES (%s, %s, %s, %s)", \
                        (int(message_id), int(user_id), int(room_id), mention_ids))
        except psycopg2.Error as e: 
            print("Error: Inserting Rows")
            print (e)

    # close connection
    conn.close()

if __name__ == "__main__":
    generate_sample_data("", "")    
