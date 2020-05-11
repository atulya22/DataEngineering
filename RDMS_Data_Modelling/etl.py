import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Reads the songs json file and converts it into a dataframe 
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)
    df = df.set_index('num_songs')
    
    insert_song_data(df, cur)
    insert_artist_data(df, cur)

    
def insert_song_data(df, cur):
    """
    - Extracts the songs metadata
    - Insert metadata into songs table
    """
    df_song = df[['song_id', 'title', 'artist_id', 'year', 'duration']] 
    song_data = df_song.values[0]
    song_data = list(song_data)
    cur.execute(song_table_insert, song_data)


def insert_artist_data(df, cur):
    """
    -  Extracts the artist metadata
    -  Insert metadata into artist table
    """
    df_artist = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    df_artist = df_artist.values
    artist_data = df_artist[0]
    artist_data = list(artist_data)
    cur.execute(artist_table_insert, artist_data)
    

def process_log_file(cur, filepath):
    """
    - Reads the logs file and converts it into a dataframe
    - Filters the dataframe by NextSong action
    """
    
    # open log file
    
    df = pd.read_json(filepath, lines=True)
    
    # filter by NextSong action
    df = df[df['page'] == 'NextSong']
    
    insert_time_data(df, cur)
    
    insert_user_data(df, cur)
    
    insert_songplays_data(df, cur)

    
def insert_time_data(df, cur):
    """
    - Extracts time data
    - Inserts data into time table
    """
    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    t.head() 
    
    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['start_time', 'hour', 'day', 'weekofyear', 'month', 'year', 'weekday']
    time_dict = {}

    for v, w in zip(time_data, column_labels):
        time_dict[w] = v
        
    time_df = pd.DataFrame.from_dict(time_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
        

def insert_user_data(df, cur):
    """
    - Extract user data from dataframe
    - Insert data into user table
    """
    
    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']] 

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

        
def insert_songplays_data(df, cur):
    """
    - Extracts song id and artist id by matching on song title, artist name and song duration
    - Extracts metadata about user activity
    - Inserts the extracted data into the songplays table
    """
    # insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        start_time = pd.to_datetime(row.ts, unit='ms')

        # insert songplay record
        songplay_data = [start_time, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)
        

def process_data(cur, conn, filepath, func):
    """
      - Loops through all the files and processes it using the given function
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files:
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
    - Starting point for the ETL pipeline
    - Connects to the database
    - Begins processing the data
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    conn.set_session(autocommit=True)
    
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()