import configparser


# CONFIG
config_file_path = 'dwh.cfg'
config = configparser.ConfigParser()
config.read_file(open(config_file_path))

REGION = config.get("CLUSTER","DWH_REGION")
ROLE_ARN = config.get("CLUSTER", "DWH_ROLE_ARN")
LOG_DATA = config.get("S3_BUCKET", "LOG_DATA")
LOG_JSONPATH = config.get("S3_BUCKET", "LOG_JSONPATH")
SONG_DATA = config.get("S3_BUCKET", "SONG_DATA")


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays "
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INT,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts TIMESTAMP,
    userAgent VARCHAR,
    userId INT
    )
"""
)


staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs 
    (num_songs INT,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INT
    )
""")


user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
    (
    user_id int PRIMARY KEY sortkey,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar
    )
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
    (
    song_id varchar PRIMARY KEY sortkey,
    title varchar, 
    artist_id varchar,
    year int,
    duration numeric
    )
""")


artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
    (
    artist_id varchar PRIMARY KEY sortkey,
    name varchar, 
    location varchar, 
    latitude numeric,
    longitude numeric
    )
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
    (
    start_time timestamp PRIMARY KEY sortkey distkey,
    hour int, 
    day int,
    week int,
    month int,
    year int,
    weekday int
    )
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay
    (
    songplay_id INT IDENTITY(0,1) primary key sortkey,
    start_time timestamp distkey,
    user_id int ,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id int,
    location varchar,
    user_agent varchar,
    CONSTRAINT fk_time 
        FOREIGN KEY (start_time) REFERENCES time(start_time),
    CONSTRAINT fk_users 
        FOREIGN KEY (user_id) REFERENCES users(user_id),
    CONSTRAINT fk_songs 
        FOREIGN KEY (song_id) REFERENCES songs(song_id),
    CONSTRAINT fk_artists 
        FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
    )
""")


# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {s3_bucket}
    CREDENTIALS 'aws_iam_role={arn}'
    REGION '{region}'
    FORMAT  JSON {json_path}
    TIMEFORMAT  'epochmillisecs'
""").format(s3_bucket=LOG_DATA, arn=ROLE_ARN, region=REGION,json_path=LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs
    FROM {s3_bucket}
    CREDENTIALS 'aws_iam_role={arn}'
    REGION  '{region}'
    FORMAT  JSON 'auto'
""").format(s3_bucket=SONG_DATA, arn=ROLE_ARN, region=REGION)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    (SELECT DISTINCT 
    a.ts ,
    a.userId ,
    a.level ,
    b.song_id ,
    b.artist_id ,
    a.sessionId ,
    a.location ,
    a.userAgent 
    FROM staging_events a
        LEFT JOIN staging_songs b
        ON a.song = b.title AND a.artist = b.artist_name and a.length = b.duration
    WHERE a.page = 'NextSong')
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
    (SELECT DISTINCT 
    userId ,
    firstName ,
    lastName ,
    gender,
    level
    FROM staging_events
    WHERE userId IS NOT NULL
    AND page = 'NextSong')
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
    (SELECT DISTINCT 
    song_id,
    title,
    artist_id,
    year,
    duration
    FROM staging_songs
    WHERE song_id IS NOT NULL)
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
    (SELECT DISTINCT 
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL)
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    (SELECT DISTINCT 
    ts,
    EXTRACT(hour FROM ts),
    EXTRACT(day FROM ts),
    EXTRACT(week FROM ts),
    EXTRACT(month FROM ts),
    EXTRACT(year FROM ts),
    EXTRACT(dayofweek FROM ts)
    FROM staging_events
    WHERE ts IS NOT NULL)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop,songplay_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
