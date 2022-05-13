CREATE DATABASE IF NOT EXISTS assign3;

use assign3;

DROP TABLE IF EXISTS events;

CREATE TABLE IF NOT EXISTS events(
artist STRING,auth STRING,
firstName STRING,gender STRING,
itemInSession int,lastName STRING,
length float,level STRING,location STRING,
method STRING,page STRING,registration float,
sessionId float,song STRING,status int,
ts int,userAgent STRING,userId int)
ROW FORMAT serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

LOAD DATA LOCAL INPATH '/employee/events.csv' INTO table events;

select sessionId ,userId
,first_value(song)over(partition by sessionId  order by song) as first
,last_value (song) over (partition by sessionId  order by song rows between unbounded preceding and unbounded following) as last
from events;

select distinct song ,userId 
,dense_rank() over ( partition by song order by userId) as ranked
from events 
order by song, userId;

select distinct song ,userId 
,row_number() over ( partition by song order by userId) as ranked
from events 
order by song, userId;

select location , artist ,count(song)
from events
group by location , artist 
GROUPING SETS (location ,()) limit 20;

select location , artist ,count(song)
from events
group by location , artist 
GROUPING SETS (location ,artist ,()) limit 20;

select userId ,song
,lead(song,1,0) over(partition by userId order by song) as next_song
,lag(song,1,0) over(partition by userId order by song) as prev_song
from events;

select userId ,song ,ts
from events
order by userId ,song ,ts;

select userId ,song ,ts
from events
CLUSTER BY  userId ,song ,ts;


