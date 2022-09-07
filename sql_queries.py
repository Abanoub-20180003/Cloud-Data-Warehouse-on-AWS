import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh2.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_sogns;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS  staging_events (
artist VARCHAR ,
auth VARCHAR,
firstName VARCHAR ,
gender char(1),
itemInSession  Integer,
lastName VARCHAR ,
length DECIMAL,
level VARCHAR ,
location VARCHAR,
method varchar,
page VARCHAR,
registration float,
sessionId INTEGER, 
song VARCHAR,
status integer,
ts varchar ,
userAgent VARCHAR,
userId INTEGER

);
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS  staging_songs (
num_songs INTEGER ,
artist_id VARCHAR ,
artist_latitude DECIMAL,
artist_longitude DECIMAL,
artist_location VARCHAR,
artist_name VARCHAR ,
song_id VARCHAR,
title VARCHAR,
duration DECIMAL,
year INTEGER 
);
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int GENERATED ALWAYS AS IDENTITY, 
    start_time TIMESTAMP  sortkey, 
    user_id INTEGER , 
    level VARCHAR , 
    song_id VARCHAR , 
    artist_id VARCHAR , 
    session_id INTEGER , 
    location VARCHAR , 
    user_agent VARCHAR 
);
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users(
    user_id INTEGER NOT NULL sortkey, 
    first_name VARCHAR , 
    last_name VARCHAR , 
    gender CHAR(1) , 
    level VARCHAR 
);
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR NOT NULL sortkey, 
    title VARCHAR , 
    artist_id VARCHAR  distkey, 
    year INTEGER  , 
    duration DECIMAL 
);
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR  sortkey, 
    name VARCHAR  , 
    location VARCHAR , 
    lattitude Decimal, 
    longitude Decimal
);
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP NOT NULL sortkey, 
    hour INTEGER ,
    day INTEGER , 
    week INTEGER , 
    month VARCHAR , 
    year INTEGER , 
    weekday INTEGER 
);
""")




# STAGING TABLES
# Note that you do not taken all cloumns fron events fileSEE
staging_events_copy = ("""  
copy staging_events 
from '{}' 
iam_role '{}'
format as json '{}';
""").format(config.get('DWH','LOG_DATA'), config.get('DWH','ARN'), config.get('DWH','log_jsonpath'))

staging_songs_copy = (""" 
copy staging_songs 
from '{}' 
iam_role '{}'
format as json 'auto';
""").format(config.get('DWH','SONG_DATA'), config.get('DWH','ARN'))



# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS') AS start_time, 
se.userId as user_id, 
se.level as level, 
ss.song_id as song_id, 
ss.artist_id as artist_id, 
se.sessionid as session_id, 
se.location as location,  
se.userAgent as user_agent
from staging_events se
join staging_songs ss on (se.song = ss.title) and (se.artist = ss.artist_name) and (se.length = ss.duration)
AND se.page  =  'NextSong'
""")

user_table_insert = ("""  INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT 
userId as user_id, 
firstName as first_name, 
lastName as last_name, 
gender as gender, 
level as level
FROM staging_events
WHERE page = 'NextSong'
AND user_id NOT IN (SELECT DISTINCT user_id FROM users); 
""")



song_table_insert = (""" INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT(song_id) as song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)
""")

artist_table_insert = (""" INSERT INTO artists (artist_id, name, location, lattitude, longitude)
SELECT DISTINCT(artist_id) as artist_id, 
artist_name as name, 
artist_location as location, 
artist_latitude as lattitude, 
artist_longitude as longitude
FROM staging_songs
where artist_id not in (SELECT DISTINCT(artist_id) from artists);
""")

time_table_insert = (""" INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS') AS start_time, 
EXTRACT(hour FROM start_time) as hour,
EXTRACT(day FROM start_time) as day, 
EXTRACT(week FROM start_time) as week, 
EXTRACT(month FROM start_time) as month,
EXTRACT(year FROM start_time) as year, 
EXTRACT(dayofweek FROM start_time) as weekday
FROM staging_events se;
""")




analyze_queries = [
    'SELECT COUNT(*) AS total FROM songs',
    'SELECT COUNT(*) AS total FROM songplays',
    'SELECT COUNT(*) AS total FROM artists',
    'SELECT COUNT(*) AS total FROM users',
    'SELECT COUNT(*) AS total FROM time',
    'SELECT COUNT(*) AS total FROM staging_events',
    'SELECT COUNT(*) AS total FROM staging_songs'
]

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create , songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop , songplay_table_drop]


copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [ user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
