import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Description: this function to get the database cursor that using for extract data from filepath/song_file. 

    Arguments:
    cur: Get database cursor object used the cursor() method. This read-only attribute provides the Postgresql database connection used by the Cursor object.
    filepath: Provide the path to get a list of all song JSON files (this project specified data/song_data) to get songs in this list, read the song files and view the song data.

    Returns:
    Text: Returning all the paths (filepath) of the song JSON files.

   """
    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Description: this function to get the database cursor that using for extract data from filepath/log_file.

    Arguments:
    cur: Get database cursor object used the cursor() method. This read-only attribute provides the Postgresql database Connection used by the Cursor object.
    filepath: Provide the path to get a list of all JSON log files (this project specified data/log_data) to get a list of all log JSON files, read the song files and view the log data.

    Returns:
    Text: Returning all the paths (filepath) of the log JSON files.

   """
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit = 'ms')
    
    # insert time data records
    timestamp = t.values.tolist()
    hour = t.dt.hour.values.tolist()
    day = t.dt.day.values.tolist()
    week = t.dt.week.values.tolist()
    month = t.dt.month.values.tolist()
    year = t.dt.year.values.tolist()
    weekday = t.dt.weekday.values.tolist()

    time_data = list(zip(timestamp, hour, day, week, month, year, weekday))
    
    column_labels = ('timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    
    time_df = pd.DataFrame(time_data, columns= column_labels)
    
    time_df['timestamp'] = time_df['timestamp'].apply(pd.to_datetime)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        """ original part
        # insert songplay record
        songplay_data = [row.userId ,pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId , row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)
        """
        # insert songplay record
        songplay_data = [pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId , row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description: This function is responsible for listing the files in a directory,
    and then executing the ingest process for each file according to the function
    that performs the transformation to save it to the database.

    Arguments:
        cur: to get database cursor object.
        conn: to make a database connection.
        filepath: to provide the path to get a list of all JSON files (both of song_files and log_files).
        func: to provide the function to handle data

    Returns:
        None
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()