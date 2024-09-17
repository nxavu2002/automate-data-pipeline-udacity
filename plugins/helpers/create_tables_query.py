
create_tables_sql ="""
CREATE TABLE IF NOT EXISTS staging_events (
        artist              VARCHAR,
        auth                VARCHAR,
        firstName           VARCHAR,
        gender              VARCHAR(1),
        itemInSession       INT,
        lastName            VARCHAR,
        length              FLOAT,
        level               VARCHAR,
        location            TEXT,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT,
        sessionId           INT,
        song                VARCHAR,
        status              INT,
        ts                  BIGINT,
        userAgent           TEXT,
        userId              VARCHAR
);

CREATE TABLE IF NOT EXISTS staging_songs (
  song_id VARCHAR ,
  artist_id VARCHAR ,
  artist_name VARCHAR ,
  artist_location VARCHAR ,
  artist_latitude FLOAT ,
  artist_longitude FLOAT ,
  duration FLOAT ,
  num_songs INT ,
  title VARCHAR ,
  year INT 
);

CREATE TABLE IF NOT EXISTS songplays (
  songplay_id VARCHAR NOT NULL PRIMARY KEY,
  start_time TIMESTAMP,
  userId VARCHAR,
  level VARCHAR,
  song_id VARCHAR,
  artist_id VARCHAR,
  sessionId INT,
  location VARCHAR,
  userAgent VARCHAR
);

CREATE TABLE IF NOT EXISTS users (
  userId VARCHAR PRIMARY KEY,
  firstName VARCHAR,
  lastName VARCHAR,
  gender VARCHAR(1),
  level VARCHAR
);

CREATE TABLE IF NOT EXISTS songs (
  song_id VARCHAR PRIMARY KEY,
  title VARCHAR,
  artist_id VARCHAR,
  year INT,
  duration FLOAT
);

CREATE TABLE IF NOT EXISTS artists (
  artist_id VARCHAR PRIMARY KEY,
  artist_name VARCHAR,
  artist_location VARCHAR,
  artist_latitude FLOAT,
  artist_longitude FLOAT
);

CREATE TABLE IF NOT EXISTS time (
  start_time TIMESTAMP PRIMARY KEY,
  hour INT, 
  day INT,
  week INT,
  month INT,
  year INT,
  weekday INT
);
"""