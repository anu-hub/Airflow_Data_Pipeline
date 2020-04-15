class SqlQueries:
    
    # Removed md5(events.sessionid || events.start_time) songplay_id: added in Create Table "playid Integer IDENTITY NOT NULL"
    # userid is NULL in staging_events modofied the insert query
    songplay_table_insert = ("""INSERT INTO songplays (start_time,userid,level,songid,artistid,sessionid,location,user_agent)
        SELECT  events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
            WHERE songs.artist_id is not NULL AND songs.song_id is not NULL and events.start_time is not NULL
    """)

    user_table_insert = ("""INSERT INTO users (userid,first_name,last_name,gender,level)
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong' AND userId is not null
    """)

    song_table_insert = ("""INSERT INTO songs
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs where song_id is not null and artist_id is not null
    """)

    artist_table_insert = ("""INSERT INTO artists
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs where artist_id is not null
    """)

    time_table_insert = ("""INSERT INTO time
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays where start_time is not null
    """)