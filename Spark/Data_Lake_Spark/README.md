### Data Wrangling with Apache Spark

#### Summary

In this project, we build an ETL pipeline to process song data and user activity logs. We use Apache Spark to read and process the data  hosted on Amazon S3 and convert them into analytics tables using the star schema method. We write the analytics tables back to S3 as parquet files.

#### Dataset

The dataset consists of songs metadata and logs from user activity on the application. The data resides on buckets hosted in Amazon S3.

###### Song Data: 
Collection of JSON files partitioned by the first three letters of each songs track ID. Each file contains a single song track with metadata about the song and the song's artist. `s3://udacity-dend/song_data`

##### Log Data: 
Collection of JSON files containing logs about user activity on the application. It is partitioned by year and month. `s3://udacity-dend/log_data`

#### Files

* etl.py: Script that starts the ETL process to read and write tables from/to Amazon S3 
* dl.cfg: Used to store AWS credentials 
* data/: Subset of the S3 dataset used for testing 

#### How to run

* Create a bucket on Amazon S3 bucket to store ouput (analytics) tables from the spark jon
* Run ETL.py script to initiate the ETL process
`python etl.py`

#### Analytics tables

###### songplays(songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
###### users(user_id, first_name, last_name, gender, level)
###### songs(song_id, title, artist_id, year, duration)
###### artists(artist_id, name, location, latitude, longtitude)
###### time(start_time, hour, day, week, month, year, weekday)