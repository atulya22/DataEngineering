### Data Warehouse for Music Streaming Application

#### Summary
In this project, we build an ETL pipeline that extracts songs and user activity logs from Amazon S3 and loads them onto staging tables in Amazon Redshift. The staging tables are then transformed and loaded into fact and dimension tables in Redshift. The final fact and dimsension tables help provide key insights about user activity in the application
#### Dataset
The dataset consists of songs metadata and logs from user activity on the application. 
The data resides on buckets hosted in Amazon S3.
**Song Data**: Collection of JSON files partitioned by the first three letters of each songs track ID. Each file contains a single song track with metadata about the song and the song's artist. 
`s3://udacity-dend/song_data`
**Log Data**: Collection of JSON files containing logs about user activity on the application. It is partitioned by year and month.
 `s3://udacity-dend/log_data`
**Log data mappings**: JSON file to map log data to approriate column in the staging tables
`s3://udacity-dend/log_json_path.json`

#### Architecture
##### Files
* sql_queries.py: Contains SQL statements create and update staging and dimension tables
* create_tables.py: Drops and creates tables on Redshift
* etl.py: Script that initiates ETL process on Redshift tables

##### Redshift Cluster

The project uses a **4 node DC2.large cluster**. The cluster has an IAM role set to **AmazonS3ReadOnlyAccess**.

##### How to run
Run create_tables.py to initiate a Redshift connection and create the staging and dimension tables
`python create_tables.py`
Run etl.py to copy data from S3 buckets to the staging tables in Redshift and finally transform and load the data from the staging tables onto the dimension tables.
`python etl.py`
##### Results