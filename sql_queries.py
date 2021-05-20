# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
songs_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

############### CREATE songplay table

create_songplay_table = ("""

CREATE TABLE songplays (

        songplay_id INT AUTO_INCREMENT PRIMARY KEY,
        start_time TIMESTAMP,
        user_id INT,
        level VARCHAR(10),
        song_id VARCHAR(20),
        artist_id VARCHAR(20),
        session_id VARCHAR(20),
        location VARCHAR (200),
        user_agent VARCHAR(256)
);
""")

############### CREATE user table

create_user_table = ("""

CREATE TABLE user (

    user_id INT PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender VARCHAR(1),
    level VARCHAR(10)
);
""")

############### CREATE songs table

create_songs_table = ("""

CREATE TABLE songs (

    song_id VARCHAR(20) PRIMARY KEY,
    title VARCHAR (255),
    artist_id VARCHAR (20) NOT NULL,
    year INT,
    duration FLOAT(5)
);
""")

############### CREATE artist table

create_artist_table = ("""

CREATE TABLE artist (

    artist_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(256),
    location VARCHAR(200),
    latitude FLOAT(5),
    longitude FLOAT(5)

);
""")

############### CREATE time table

create_time_table = ("""

CREATE TABLE time (

    start_time TIMESTAMP PRIMARY KEY,
    hour INT, 
    day INT,
    week INT, 
    month INT,
    year INT, 
    weekday INT
);

""")


# INSERT RECORDS

songplay_table_insert = ("""

INSERT IGNORE INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id,session_id, location, user_agent)
 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""

INSERT IGNORE INTO user (user_id, first_name, last_name, gender, level)
 VALUES (%s, %s, %s, %s, %s)
""")

song_table_insert = ("""

INSERT IGNORE INTO songs (song_id, title, artist_id, year, duration)
 VALUES (%s, %s, %s, %s, %s)
""")

artist_table_insert = ("""

INSERT IGNORE INTO artist (artist_id, name, location, latitude, longitude)
 VALUES (%s, %s, %s, %s, %s)
""")

time_table_insert = ("""

INSERT IGNORE INTO time (start_time, hour, day, week, month, year, weekday)
 VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS 

find_song = ("""

SELECT 
    FROM songs ss
JOIN artist ar
    ON ar.artist_id = ss.artist_id
WHERE ss.title = %s
AND ar.name = %s
AND ss.duration = %s;
""")

#Song select
song_select = ("""
SELECT ss.song_id, ss.artist_id FROM songs ss 
JOIN artist ars on ss.artist_id = ars.artist_id
WHERE ss.title = %s
AND ars.name = %s
AND ss.duration = %s
;
""")


# QUERY LIST

create_tables_queries = [create_songplay_table, create_songs_table,
                      create_artist_table, create_user_table, create_time_table]

drop_tables_queries = [songplay_table_drop, songs_table_drop,
                       artist_table_drop, user_table_drop, time_table_drop]



