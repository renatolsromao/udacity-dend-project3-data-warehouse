import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE staging_events (
        artist TEXT,
        auth TEXT,
        first_name TEXT,
        gender CHAR(1),
        session_item INT,
        last_name TEXT,
        length NUMERIC(10, 5),
        level VARCHAR(10),
        location TEXT,
        method CHAR(3),
        page TEXT,
        registration TEXT,
        session_id INT,
        song TEXT,
        status int,
        ts TIMESTAMP,
        user_agent TEXT,
        user_id INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs INT,
        artist_id TEXT, 
        artist_latitude NUMERIC(15, 5), 
        artist_longitude NUMERIC(15, 5), 
        artist_location TEXT, 
        artist_name TEXT, 
        song_id TEXT, 
        title TEXT, 
        duration NUMERIC(10, 5), 
        year INT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id INT NOT NULL,
        level VARCHAR(10),
        song_id TEXT NOT NULL,
        artist_id TEXT NOT NULL,
        session_id INT,
        location TEXT,
        user_agent TEXT,
        FOREIGN KEY(start_time) REFERENCES time(start_time),
        FOREIGN KEY(user_id) REFERENCES users(user_id),
        FOREIGN KEY(song_id) REFERENCES songs(song_id),
        FOREIGN KEY(artist_id) REFERENCES artists(artist_id)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender CHAR(1),
        level VARCHAR(10)
    )
    DISTSTYLE ALL;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id TEXT PRIMARY KEY,
        title TEXT,
        artist_id TEXT NOT NULL,
        year INT,
        duration NUMERIC(10, 5)
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS  artists (
        artist_id TEXT PRIMARY KEY,
        name TEXT,
        location TEXT,
        latitude NUMERIC(15, 5),
        longitude NUMERIC(15, 5)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS  time (
        start_time timestamp PRIMARY KEY, 
        hour INT, 
        day INT, 
        week INT, 
        month INT,  
        year INT, 
        weekday INT
    )
    DISTSTYLE ALL;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
    FROM '{s3_path}'
    iam_role '{iam_role}' region '{aws_region}'
    json '{json_path}'
    TIMEFORMAT as 'epochmillisecs';
""").format(s3_path=config['S3']['LOG_DATA'], json_path=config['S3']['LOG_JSONPATH'],
            iam_role=config['IAM_ROLE']['ARN'], aws_region=config['AWS']['REGION'])

staging_songs_copy = ("""
    COPY staging_songs
    FROM '{s3_path}'
    iam_role '{iam_role}' region '{aws_region}'
    json 'auto'
    TIMEFORMAT as 'epochmillisecs';
""").format(s3_path=config['S3']['SONG_DATA'], iam_role=config['IAM_ROLE']['ARN'], aws_region=config['AWS']['REGION'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays 
    (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)  
    SELECT DISTINCT
        e.ts as start_time,
        e.user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.session_id,
        e.location,
        e.user_agent
    FROM
        staging_events AS e
        LEFT JOIN staging_songs AS s
            ON (e.song = s.title) AND (e.artist = s.artist_name)
    WHERE 
        e.page = 'NextSong'
    ; 
""")

user_table_insert = ("""
    INSERT INTO users
    (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts DESC) AS rn
        FROM staging_events
        WHERE user_id IS NOT NULL AND page = 'NextSong'
    ) AS e
    WHERE rn = 1
    ;
""")

song_table_insert = ("""
    INSERT INTO songs
    (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id, title, artist_id, year, duration
    FROM 
        staging_songs
    WHERE 
        song_id IS NOT NULL
    ;
""")

artist_table_insert = ("""
    INSERT INTO artists
    (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY YEAR DESC) AS rn
        FROM staging_songs
        WHERE artist_id IS NOT NULL
    ) AS s
    WHERE
        rn = 1
    ;
""")

time_table_insert = ("""
    INSERT INTO time
    (start_time, hour, day, week, month, year, weekday)
    SELECT
        ts AS start_time, 
        DATE_PART(h, ts) AS hour,
        DATE_PART(d, ts) AS day,
        DATE_PART(w, ts) AS week,
        DATE_PART(mon, ts) AS month,
        DATE_PART(y, ts) AS year,
        DATE_PART(dow, ts) AS weekday
    FROM 
        (SELECT DISTINCT ts FROM staging_events)
    ;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
