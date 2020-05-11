# Project Summary
The aim of this project is to setup a Postgres database with data collected from a music streaming application. Setting up such a databse will provide key insight about user activity on the application.

# Dataset
The dataset consists of songs metadata and logs from user activity on the application.
**Songs Data**: Collection of JSON files partitioned by the first three letters of each songs track ID. Each file contains a single song track with metadata about the song and the song's artist.
**Logs Data**: Collection of JSON files containing logs about user activity on the application. It is partitioned by year and month.

# Project Structure
* data/log_data: Contains user activity logs
* data/song_data: Contains songs and artist metadata
* sql_queries.py: Contains SQL statements to define, manipulate and query tables
* create_tables.py: Creates databases and related tables
* etl.ipynb: Exploratory notebook to develop the ETL process
* etl.py : Python script to perform ETL and load the database with all the data
* test.ipynb: Notebook to validate whether the tables in the database are being populated

#### How To Execute Scripts:

First run create_tables.py to setup the Sparkify database and create the fact and dimension tables
`python create_tables.py`

Run etl.py to perform ETL on the JSON files and populate the tables
`python etl.py`


# Results
#### Fact table
##### Songplays
#
![None](https://github.com/atulya22/DataEngineering/blob/master/RDMS_Data_Modelling/assets/FactTable.jpg)

#### Dimension Tables
##### Users

![None](https://github.com/atulya22/DataEngineering/blob/master/RDMS_Data_Modelling/assets/Dim_User.jpg)

##### Songs

![None](https://github.com/atulya22/DataEngineering/blob/master/RDMS_Data_Modelling/assets/Dim_Songs.jpg)

##### Artist

![None](https://github.com/atulya22/DataEngineering/blob/master/RDMS_Data_Modelling/assets/Dim_Artist.jpg)

##### Time

![None](https://github.com/atulya22/DataEngineering/blob/master/RDMS_Data_Modelling/assets/Dim_Time.jpg)
