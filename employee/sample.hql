use assign1_nehad;

DROP TABLE IF EXISTS songs;

CREATE TABLE IF NOT EXISTS songs(
artist_id String,artist_latitude float,
artist_location String,artist_longitude float,
artist_name String,duration float,
num_songs int,song_id String,
title String,year int)
ROW FORMAT serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

LOAD DATA LOCAL INPATH '/employee/songs.csv' INTO table songs;

