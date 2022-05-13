CREATE DATABASE IF NOT EXISTS assign1_nehad;

DESCRIBE DATABASE EXTENDED assign1_nehad;

CREATE DATABASE IF NOT EXISTS assign_loc_nehad LOCATION '/hp_db/assign_loc_nehad';

use assign1_nehad;

DROP TABLE IF EXISTS assign1_intern_table;

CREATE TABLE IF NOT EXISTS assign1_intern_table ( eid int, name String,
age int,job String,department int
,city String, salary int , manager_id int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

describe formatted assign1_intern_table;

LOAD DATA LOCAL INPATH '/employee/employee.csv' INTO table assign1_intern_table;

select * from assign1_intern_table;

use assign_loc_nehad;

DROP TABLE IF EXISTS assign1_extern_tab;

CREATE EXTERNAL TABLE IF NOT EXISTS assign1_extern_tab( eid int, name String,
age int,job String,department int
,city String, salary int , manager_id int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/nehad';

describe formatted assign1_extern_tab ;

!hdfs dfs -rm -f -r /nehad;
!hdfs dfs -mkdir /nehad;
!hdfs dfs -put /employee/employee.csv /nehad;
!hdfs dfs -ls /nehad;

use assign1_nehad;

DROP TABLE IF EXISTS assign1_intern_table;

use assign_loc_nehad;

DROP TABLE IF EXISTS assign1_extern_table;

use assign1_nehad;

CREATE TABLE IF NOT EXISTS assign1_intern_table( eid int, name String,
    age int,job String,department int
    ,city String, salary int , manager_id int)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ',';
	
LOAD DATA LOCAL INPATH '/employee/employee.csv' INTO table assign1_intern_table;

use assign_loc_nehad;

CREATE EXTERNAL TABLE IF NOT EXISTS assign1_extern_table  ( eid int, name String,
	age int,job String,department int
	,city String, salary int , manager_id int)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ','
	LOCATION '/nehad';

use assign1_nehad;

CREATE TABLE IF NOT EXISTS staging ( eid int, name String,
	age int,job String,department int
	,city String, salary int , manager_id int)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/employee/employee.csv' INTO table staging;

insert into table assign1_intern_table
select * from staging;

use assign_loc_nehad;

insert into table assign1_extern_table
select * from assign1_nehad.staging;

!wc -l songs.csv;

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

Select * from songs;

select count(*) from songs;

use assign_loc_nehad;

DROP TABLE IF EXISTS songs_ext;

CREATE EXTERNAL TABLE IF NOT EXISTS songs_ext(
artist_id String,artist_latitude float,
artist_location String,artist_longitude float,
artist_name String,duration float,
num_songs int,song_id String,
title String,year int)
ROW FORMAT serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/nehad/songs';

!hdfs dfs -rm -f -r /nehad/songs;
!hdfs dfs -mkdir /nehad/songs;
!hdfs dfs -put /employee/songs.csv /nehad/songs;
!hdfs dfs -ls /nehad/songs;

select * from songs_ext;

DROP TABLE IF EXISTS assign_loc_nehad.assign1_intern_table;

alter table assign1_nehad.assign1_intern_table rename to assign_loc_nehad.assign1_intern_table;

describe formatted assign_loc_nehad.assign1_intern_table;


