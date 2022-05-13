CREATE DATABASE IF NOT EXISTS assign2;

use assign2;

DROP TABLE IF EXISTS songs;

CREATE EXTERNAL TABLE IF NOT EXISTS songs(
artist_id String,artist_latitude float,
artist_location String,artist_longitude float,
duration float,num_songs int,song_id String,title String)
PARTITIONED BY ( year int, artist_name string)
ROW FORMAT serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/songs';

!hdfs dfs -rm -f -r /songs;
!hdfs dfs -mkdir /songs;
!hdfs dfs -put /employee/songs.csv /songs;
!hdfs dfs -ls /songs;

select * from songs;

ALTER TABLE songs DROP IF EXISTS PARTITION (artist_name='Tom Petty',year='1994');

alter table songs add partition (artist_name='Tom Petty',year='1994')
location '/nehad/songs';

!hdfs dfs -rm -f -r /nehad/songs;
!hdfs dfs -mkdir /nehad/songs;
!hdfs dfs -put /employee/songs.csv /nehad/songs;
!hdfs dfs -ls /nehad/songs;

show partitions songs;

DROP TABLE IF EXISTS staging;

CREATE TABLE IF NOT EXISTS staging(
artist_id String,artist_latitude float,
artist_location String,artist_longitude float,
artist_name String,duration float,
num_songs int,song_id String,
title String,year int)
ROW FORMAT serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

LOAD DATA LOCAL INPATH '/employee/songs.csv' INTO table staging;

set hive.exec.dynamic.partition.mode=nonstrict;

from staging
insert overwrite table songs partition (year ,artist_name)
select *;

select * from songs;

ALTER TABLE songs SET TBLPROPERTIES('EXTERNAL'='FALSE');

truncate table songs;

ALTER TABLE songs SET TBLPROPERTIES('EXTERNAL'='TRUE');

from staging
insert overwrite table songs partition (year,artist_name)
select *;

ALTER TABLE songs SET TBLPROPERTIES('EXTERNAL'='FALSE'); 

truncate table songs;

ALTER TABLE songs SET TBLPROPERTIES('EXTERNAL'='TRUE');

from staging
insert overwrite table songs partition (year = 1994,artist_name)
select artist_id, artist_latitude, artist_location, artist_longitude, 
duration, num_songs , song_id , title string, artist_name 
 where year = 1994;
 
from staging
insert overwrite table songs partition (year = 2005,artist_name)
select artist_id, artist_latitude, artist_location, artist_longitude, 
duration, num_songs , song_id , title string, artist_name 
 where year = 2005;

from staging
insert overwrite table songs partition (year = 1987,artist_name)
select artist_id, artist_latitude, artist_location, artist_longitude, 
duration, num_songs , song_id , title string, artist_name 
 where year = 1987;
 
select * from songs; 

DROP TABLE IF EXISTS avro_staging;

CREATE TABLE avro_staging LIKE staging
STORED AS AVRO;

DROP TABLE IF EXISTS parquet_staging;

CREATE TABLE parquet_staging LIKE staging
STORED AS PARQUET;





