import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events (id int IDENTITY(1,1) PRIMARY KEY, artist text, auth varchar(12),\
first_name varchar(25), gender varchar(5), item_in_session smallint, last_name varchar(25), length float, level varchar(10), location varchar(50),\
method varchar(10), page varchar(50), registration bigint, session_id int, song text, status int, ts bigint,\
user_agent text, user_id int)
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs (id int IDENTITY(0,1) PRIMARY KEY, num_songs integer, artist_id varchar(25),\
artist_latitude float, artist_longitude float, artist_location text, artist_name text NOT NULL,\
song_id varchar(25) NOT NULL, title text, duration float, year int)
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays (songplay_id int IDENTITY(0,1) PRIMARY KEY, start_time timestamp, user_id int NOT NULL,\
level varchar(10), song_id varchar(25) NOT NULL, artist_id varchar(25) NOT NULL, session_id int NOT NULL, location varchar(50), user_agent text)
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY, first_name varchar(25), last_name varchar(25), gender varchar(2),\
level varchar(10))
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs (song_id varchar(25) PRIMARY KEY, title text,\
artist_id varchar(50), year int, duration numeric)
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists (artist_id varchar(25) PRIMARY KEY, name varchar(200), location varchar(200),\
latitude float, longitude float)
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY, hour smallint, day smallint, week smallint, month smallint, year smallint, weekday smallint)
""")

# STAGING TABLES

staging_events_copy = (""" 
copy staging_events from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
JSON 's3://udacity-dend/log_json_path.json'
""").format(config.get('S3', 'LOG_DATA' ),config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = ("""
copy staging_songs from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
JSON 'auto'
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
select distinct DATEADD(MS, se.ts, '1970-01-01') as start_time, se.user_id as user_id, se.level, ss.song_id as song_id, ss.artist_id as artist_id,\
se.session_id as session_id, se.location as location, se.user_agent as user_agent from staging_songs ss,\
staging_events se where ss.artist_name= se.artist and ss.title=se.song 
and se.page='NextSong' 
and se.user_id not in (select distinct s.user_id from songplays s where s.user_id = se.user_id and s.session_id = se.session_id and s.start_time = start_time);

""")

user_table_insert = (""" INSERT INTO users (user_id, first_name, last_name, gender, level)

select distinct user_id, first_name, last_name, gender, level from staging_events where page='NextSong' and user_id is NOT NULL and 
user_id not in (select distinct user_id from users);

""")

song_table_insert = (""" INSERT INTO songs (song_id, title, artist_id, year, duration)
select distinct song_id, title, artist_id, year, duration from staging_songs where song_id not in
(select distinct song_id from songs);
""")

artist_table_insert = (""" INSERT INTO artists(artist_id, name, location, latitude, longitude)
select distinct artist_id, artist_name as name, artist_location as location, artist_latitude as latitude, artist_longitude as longitude
from staging_songs where artist_id not in (select distinct artist_id from artists)
""")

# time_table_insert = (""" INSERT INTO time(start_time, hour, day, week, month, year, weekday)
# select start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time),
# extract(month from start_time), extract(year from start_time), extract(dow FROM start_time) AS weekday
# from songplays where start_time not in (select distinct start_time from time); """)


time_table_insert = (""" INSERT INTO time(start_time, hour, day, week, month, year, weekday)
select distinct DATEADD(MS, ts, '1970-01-01') as start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time),
extract(month from start_time), extract(year from start_time), extract(dow FROM start_time) AS weekday
from staging_events where page='NextSong' and start_time not in (select distinct start_time from time); """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
