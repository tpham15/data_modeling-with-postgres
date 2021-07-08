import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Process a song file:
    - Get song information from the file and insert into 'song' table
    - Get artist information from the file and insert into 'artist' table
    """
    # open song file
    try:
        df = pd.read_json(filepath, lines=True)
    except psycopg2.Error as e:
        print("Error: Can not read_json file {}" .format(filepath))
        print(e)
    

    # insert song record
    try:
        #song_data = (df['song_id'], df['title'], df['artist_id'], df['year'], df['duration'])
        song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])

    except psycopg2.Error as e:
        print("Error: Can not insert data into song_data")
        print(e)
        
    try:
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as e:
        print("Error: Can not insert song_data into song_table")
        print(e)
    
    # insert artist record
    try:
        #artist_data = (df['artist_id'], df['artist_name'], df['artist_location'], df['artist_latitude'], df['artist_longitude'])
        artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    except psycopg2.Error as e:
        print("Error: Can not insert data to artist_data")
        print(e)
        
    try: 
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.Error as e:
        print("Error: Can not insert artist_data into artist_table")
        print(e)


def process_log_file(cur, filepath):
    """
    Process a log file:
    - Get time data from the log file and insert into 'time' table
    - Get user data from the log file and insert into 'user' table
    - Get current playing song data and insert into 'songplay' table
    """
    # open log file
    try:
        df = pd.read_json(filepath, lines=True)
    except psycopg2.Error as e:
        print("Error: Can not read_json from file {}" .format(filepath))
        print(e)

    # filter by NextSong action
    try:
        df = df[df['page'] == 'NextSong']
    except psycopg2.Error as e:
        print("Error: Can not get 'NextSong' page")
        print(e)

    # convert timestamp column to datetime
    try:
        t = pd.to_datetime(df['ts'], unit='ms')
    except psycopg2.Error as e:
        print("Error: Can not convert timestamp to datetime")
        print(e)
    
    # insert time data records
    try:
        time_data = [t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.dayofweek]
    except psycopg2.Error as e:
        print("Error: Can not insert data into time_data")
        print(e)
    
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    
    try:
        time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
    except psycopg2.Error as e:
        print("Error: Can not convert time_data to time_df DataFrame")
        print(e)

    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except psycopg2.Error as e:
            print("Error: Can not insert time_df into time_table")
            print(e)

    # load user table
    user_columns = ('user_id', 'first_name', 'last_name', 'gender', 'level')
    
    try:
        user_data = [df['userId'], df['firstName'], df['lastName'], df['gender'], df['level']]
    except psycopg2.Error as e:
        print("Error: Can not get user_data")
        print(e)
        
    try:
        user_df = pd.DataFrame(dict(zip(user_columns, user_data)))
    except psycopg2.Error as e:
        print("Error: Can not convert user_data to user_df DataFrame")
        print(e)    
    

    # insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row)
        except psycopg2.Error as e:
            print("Error: Can not insert user_df into user_table")
            print(e)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        try:
            cur.execute(song_select, (row.song, row.artist, row.length))
        except psycopg2.Error as e:
            print("Error: song_select query failed")
            print(e)
            
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        try:
            songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        except psycopg2.Error as e:
            print("Error: Can not get songplay_data")
            print(e)
        
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e:
            print("Error: Can not insert songplay_data into songplay_table")
            print(e)


def process_data(cur, conn, filepath, func):
    """
    Process JSON files for a data direcotry path
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Main function
    - Create connection to sparkifydb database
    - Process Song and Log data
    - Close the cursor and database connection
    """
    try:
        conn = psycopg2.connect("dbname=sparkifydb user=postgres password=Bakhoamethu123")
    except psycopg2.Error as e:
        print("Error: Can not connect to sparkifydb")
        print(e)
    
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Can not get cursor")
        print(e)

    process_data(cur, conn, filepath='/Users/vienpham/Desktop/project_result/data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='/Users/vienpham/Desktop/project_result/data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()