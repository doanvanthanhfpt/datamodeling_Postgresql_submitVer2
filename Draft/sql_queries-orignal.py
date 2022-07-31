# DROP TABLES

songplay_table_drop = "songplays"
user_table_drop = "user"
song_table_drop = "song"
artist_table_drop = "artist"
time_table_drop = "time"

# CREATE TABLES

# Fact table
# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_create = ("""
""")

# Dimension Tables

# user_id, first_name, last_name, gender, level
user_table_create = ("""
""")

# song_id, title, artist_id, year, duration
song_table_create = ("""
""")

# artist_id, name, location, latitude, longitude
artist_table_create = ("""
""")

# start_time, hour, day, week, month, year, weekday
time_table_create = ("""
""")

# INSERT RECORDS

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")


time_table_insert = ("""
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]