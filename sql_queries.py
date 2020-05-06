import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP STAGING TABLE

staging_event_table_drop="DROP TABLE IF EXISTS staging_events"
staging_song_table_drop="DROP TABLE IF EXISTS staging_songs"

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
artist varchar,
auth varchar,
firstname varchar,
gender varchar,
iteminsession int,
lastname varchar,
length float,
level varchar,
location varchar,
method varchar,
page varchar,
registration float,
sessionid int,
song text,
status int,
ts float,
useragent text,
userid int
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
song_id varchar PRIMARY KEY,
num_songs int, 
artist_id varchar,
artist_latitude float,
artist_longtitude float,
artist_location varchar,
artist_name varchar, 
title varchar, 
duration float, 
year int
);
""")


songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (
songplay_id SERIAL PRIMARY KEY, 
start_time bigint NOT NULL,
user_id int NOT NULL, 
level varchar NOT NULL, 
song_id varchar NOT NULL, 
artist_id varchar NOT NULL, 
session_id int NOT NULL,
location varchar NOT NULL,
user_agent varchar NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
user_id int NOT NULL PRIMARY KEY, 
first_name varchar NOT NULL,
last_name varcharNOT NULL,
gender varchar NOT NULL, 
level varchar NOT NULL
)
diststyle all;
""")

song_table_create =("""
CREATE TABLE IF NOT EXISTS songs(
song_id varchar NOT NULL PRIMARY KEY, 
title varchar NOT NULL,
artist_id varchar NOT NULL,
year int,
duration float
)
diststyle all;
""")

artist_table_create =("""
CREATE TABLE IF NOT EXISTS artists(
artist_id varchar NOT NULL PRIMARY KEY,
name varchar NOT NULL,
location varchar,
lattitude float,
longitude float
)
diststyle all;
""")

time_table_create =("""
CREATE TABLE IF NOT EXISTS time(
start_time bigint PRIMARY KEY,
hour int, 
day int,
week int,
month int, 
year int,
weekday int
)
diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
from '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
FORMAT AS JSON '{}';
""").format(CONFIG["S3"]["LOG_DATA"], CONFIG["IAM_ROLE"]["ARN"], CONFIG["S3"]["LOG_JSONPATH"])

staging_songs_copy = ("""
COPY staging_songs
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
JSON 'auto';
""".format(CONFIG["S3"]["SONG_DATA"], CONFIG["IAM_ROLE"]["ARN"])

# FINAL TABLES

songplay_table_insert = ("""
insert into songplay (start_time,
                       user_id,
                       level,
                       song_id,
                       artist_id,
                       session_id,
                       location,
                       user_agent)
select staging_events.ts as start_time,
       staging_events.userid::INTEGER as user_id,
       staging_events.level,
       staging_songs.song_id,
       staging_songs.artist_id,
       staging_events.sessionid as session_id,
       staging_events.location,
       staging_events.useragent as user_agent
  from staging_events
  left join staging_songs
    on staging_events.song = staging_songs.title
   and staging_events.artist = staging_songs.artist_name
  left outer join songplay
    on staging_events.userid = songplays.user_id
   and staging_events.ts = songplays.start_time
 where staging_events.page = 'NextSong'
   and staging_events.userid is not Null
   and staging_events.level is not Null
   and staging_songs.song_id is not Null
   and staging_songs.artist_id is not Null
   and staging_events.sessionid is not Null
   and staging_events.location is not Null
   and staging_events.useragent is not Null
   and songplays.songplay_id is Null
 order by start_time, user_id
;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)  
SELECT DISTINCT user_id,user_first_name,user_last_name,user_gender,user_level
    FROM staging_events
    WHERE page = 'NextSong'
    ;
"""

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
  FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
  FROM staging_songs;
""")

time_table_insert = ("""
insert into time 
(
select start_time,
       date_part(hour, date_time) as hour,
       date_part(day, date_time) as day,
       date_part(week, date_time) as week,
       date_part(month, date_time) as month,
       date_part(year, date_time) as year,
       date_part(weekday, date_time) as weekday
 from (select ts as start_time,
               '1970-01-01'::date + ts/1000 * interval '1 second' as date_time
          from staging_events
         group by ts) as temp
 order by start_time
);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
