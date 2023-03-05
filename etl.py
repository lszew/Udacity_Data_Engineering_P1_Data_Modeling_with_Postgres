import os
import glob
import io
import psycopg2
import pandas as pd

from sql_queries import *

songs_df = pd.DataFrame(columns=['song_id', 'title', 'artist_id', 'year', 'duration'])
artists_df = pd.DataFrame(columns=['artist_id', 'name', 'location', 'latitude', 'longitude'])
times_df = pd.DataFrame(columns=['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'])
users_df = pd.DataFrame(columns=['user_id', 'first_name', 'last_name', 'gender', 'level'])
songplays_df = pd.DataFrame(columns=['start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent'])


def process_song_file(cur, filepath):
    """
    load JSON song files data (song and artist) into PostgreSQL DB tables using pandas data frame

    :param cur: psycopg2 cursor
    :param filepath: file path
    
    :returns: None
    """       
    global songs_df, artists_df
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # get song data and concat it to songs data frame
    song_df = df[songs_df.columns]
    songs_df = pd.concat([songs_df, song_df], sort=False)
    
    # get artist data and concat it to artists data frame
    artist_df = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_df.columns = artists_df.columns    
    artists_df = pd.concat([artists_df, artist_df], sort=False)
    

def process_log_file(cur, filepath):
    """
    load JSON log files data (time, user and songplay) into PostgreSQL DB tables using pandas data frame

    :param cur: psycopg2 cursor
    :param filepath: file path
    
    :returns: None
    """      
    global times_df, users_df, songplays_df
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms') # to reuse it in the songplay records loop    
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # get time data and concat it to times data frame
    time_data = (t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday)
    time_df = pd.DataFrame(dict(zip(times_df.columns, time_data)))
    times_df = pd.concat([times_df, time_df], sort=False)        
    
    # get user data and concat it to users data frame
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df.columns = users_df.columns
    users_df = pd.concat([users_df, user_df], sort=False)    
    
    # get songplays data
    rows = []
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            
        rows.append({
            'start_time': row.ts,
            'user_id': row.userId,
            'level': row.level,
            'song_id': songid,
            'artist_id': artistid,
            'session_id': row.sessionId,
            'location': row.location,
            'user_agent': row.userAgent
        })

    # concat songplay data to songplays data frame
    songplay_df = pd.DataFrame(rows)
    songplays_df = pd.concat([songplays_df, songplay_df], sort=False)    
        

def process_data(cur, filepath, func):
    """
    iterate through files located into filepath, call func function to process each file and print progress

    :param cur: psycopg2 cursor
    :param filepath: file path
    :param func: function to process files related to specific data
    
    :returns: None
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
        print('{}/{} files processed.'.format(i, num_files))

        
def bulk_insert_dataframe(cur, df, table):
    """
    Insert dataframe into PostgreSQL table using copy protocol

    :param cur: psycopg2 cursor
    :param df: source dataframe
    :table: target PostgreSQL table
    
    :returns: None
    """
    df.replace('', 'NULL', inplace=True)    
    sio = io.StringIO()
    sio.write(df.to_csv(index=None, header=None, na_rep='NULL', sep='|'))
    sio.seek(0) # reset the position to the start of the stream
    cur.copy_from(sio, table, columns=df.columns, sep='|', null='NULL')
    
    
def bulk_insert_dataframes(cur): 
    """
    remove duplicates and bulk insert global dataframes into their corresponding DB tables

    :param cur: psycopg2 cursor
    
    :returns: None
    """      
    global songs_df, artists_df, times_df, users_df, songplays_df
    
    songs_df.drop_duplicates(subset=['song_id'], keep='last', inplace=True)
    bulk_insert_dataframe(cur, songs_df, 'songs')
    
    artists_df.drop_duplicates(subset=['artist_id'], keep='last', inplace=True)
    bulk_insert_dataframe(cur, artists_df, 'artists')    
    
    times_df.drop_duplicates(subset=['start_time'], keep='last', inplace=True)
    bulk_insert_dataframe(cur, times_df, 'time')
    
    users_df['user_id'] = users_df['user_id'].astype(str) # to remove all duplicates
    users_df.drop_duplicates(subset=['user_id'], keep='last', inplace=True)
    bulk_insert_dataframe(cur, users_df, 'users')
    
    bulk_insert_dataframe(cur, songplays_df, 'songplays')    
    
    
def main():
    """
    open database connection and process data located in data/song_data and data/log_data
    
    :returns: None
    """        
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, filepath='data/song_data', func=process_song_file)    
    
    process_data(cur, filepath='data/log_data', func=process_log_file)
    
    bulk_insert_dataframes(cur)
    
    conn.commit()
    
    conn.close()

if __name__ == "__main__":
    main()