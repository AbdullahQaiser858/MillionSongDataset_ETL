from codecs import ignore_errors
import os
import glob
from re import X
import mysql.connector
import pandas as pd
import tables
import numpy as np
from sql_queries import *

x = 0

def isnan(value):
    try:
        import math
        return math.isnan(float(value))
    except:
        return False

# function to get all .h5 or json filepaths
def get_files(filepath, format):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, f'*.{format}'))
        for f in files:
            all_files.append(os.path.abspath(f))

    return all_files

def process_h5_file(cur, conn, filepath):
    # -> open .h5 file using filepath
    h5_file = tables.File(filepath)
    ##################################### extracting and inserting song_table data#######################################
    song_table_data = []
    # song_id
    song_table_data.append(
        str(h5_file.root.metadata.songs.cols.song_id[0]).split("'")[1])
    # title
    song_table_data.append(
        str(h5_file.root.metadata.songs.cols.title[0]).split("'")[1])
    # artist_id
    song_table_data.append(
        str(h5_file.root.metadata.songs.cols.artist_id[0]).split("'")[1])
    # year
    song_table_data.append(h5_file.root.musicbrainz.songs.cols.year[0])
    # duration
    song_table_data.append(
        h5_file.root.analysis.songs.cols.duration[0])
    # changing datatype to native python
    song_table_data[3] = song_table_data[3].item()
    song_table_data[4] = song_table_data[4].item()

    # checking for nan and replace with 'Null'
    song_table_data = [None if isnan(
        val) else val for val in song_table_data]

    # inserting artist_table_data to song table
    cur.execute(song_table_insert, song_table_data)
    ##################################### extracting and inserting artist_table data #######################################
    # artist_id
    artist_table_data = []
    artist_table_data.append(
        str(h5_file.root.metadata.songs.cols.artist_id[0]).split("'")[1])
    # artist_name
    artist_table_data.append(
        str(h5_file.root.metadata.songs.cols.artist_name[0]).split("'")[1])
    # artist_location
    artist_table_data.append(
        str(h5_file.root.metadata.songs.cols.artist_location[0]).split("'")[1])
    # latitude and longitude
    artist_table_data.append(
        h5_file.root.metadata.songs.cols.artist_latitude[0])
    artist_table_data.append(
        h5_file.root.metadata.songs.cols.artist_longitude[0])
    # converting latiude and longitude from numpy.float64 to default python int
    artist_table_data[3] = artist_table_data[3].item()
    artist_table_data[4] = artist_table_data[4].item()

    # check and replace nans
    artist_table_data = [None if isnan(
        val) else val for val in artist_table_data]

    # inserting artist_table_data into artist_table
    cur.execute(artist_table_insert, artist_table_data)
    h5_file.close()


def process_json_file(cur, conn, filepath):

    # setting a global variable for indexing

    # read in .json file
    df = pd.read_json(filepath, lines=True)
    # query result by NextSong
    df = df.query("page == 'NextSong'")

    ##################################### extracting and inserting time_table data #######################################

    df['ts'] = pd.to_datetime(df['ts'], unit='ms')

    time_list = list((df['ts'], df['ts'].dt.hour, df['ts'].dt.day, df['ts'].dt.weekofyear, df['ts'].dt.month, df['ts'].dt.year,
                      df['ts'].dt.weekday))
    columns_label = (('start_time', 'hour', 'day',
                      'week', 'month', 'year', 'weekday'))
    time_df = pd.DataFrame.from_dict(dict(zip(columns_label, time_list)))
    # insert records
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    ##################################### extracting and inserting user_table data #######################################

    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, list(row))

    ##################################### extracting and inserting songplay_table data #######################################

    for index, row in df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        global x
        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        x = x + 1
        # inserting songplay record
        songplay_record = (x, row.ts, row.userId, row.level, song_id, artist_id, row.sessionId,
                           row.location, row.userAgent)

        cur.execute(songplay_table_insert, songplay_record)


def process_data(cur, conn, filepath, func, file_type):

    file_list = get_files(filepath, file_type)
    num_files = len(file_list)
    print(f' total files found : {num_files}')

    for i, datafile in enumerate(file_list, 1):
        func(cur, conn, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():

    # # make connection to mysql local instance
    conn = mysql.connector.connect(
        host='localhost', user='root', password='32r306b')
    cur = conn.cursor()
    cur.execute("USE sparkifydb;")

    # process .h5 file
    process_data(
        cur, conn, 'M:\Projects\Data ETL with Posgres\millionsongsubset\MillionSongSubset', process_h5_file, 'h5')

    # process .json file
    process_data(cur, conn, 'M:\Projects\Data ETL with Posgres\millionsongsubset\log_data',
                 process_json_file, 'json')

    conn.close()


if __name__ == "__main__":
    main()
