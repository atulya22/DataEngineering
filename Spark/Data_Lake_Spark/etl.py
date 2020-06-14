import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id


def setup_aws():
    """
    - Read AWS credentials
    - Set up environment variables
    """
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    KEY  = config.get('AWS','KEY')
    SECRET = config.get('AWS','SECRET')

    os.environ['AWS_ACCESS_KEY_ID']= KEY
    os.environ['AWS_SECRET_ACCESS_KEY']= SECRET


def create_spark_session():
    """
    - Create spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    - Read song data from S3
    - Create songs and artist dataframes
    - Write dataframes to S3 bucket in parquet
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')

     # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').mode('overwrite').parquet(output_data+"analytics/songs_table")
    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')
    artists_table.write.parquet(output_data+"analytics/artist_table", mode='overwrite')

    

def process_log_data(spark, input_data, output_data):
    """
    - Read user activity log file from S3
    - Create user, time and songplays table dataframes
    - Write dataframes to S3 bucket in parquet
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"
    
    # output paths
    time_path = output_data + 'analytics/time_table/'
    users_path = output_data + 'analytics/users_table/'
    songplays_path = output_data + 'analytics/songplays_table/'
    
    print(log_data)
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    user_table = df.filter(df.page=='NextSong').select('userId', 'firstName', 'lastName', 'gender', 'level').dropDuplicates()

    user_table.write.parquet(users_path , mode="overwrite")

    df.show()
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x)/1000)))
    df = df.withColumn('start_time', get_datetime(df.ts))
    df.show()
    # extract columns to create time table
    time_table = df.select('start_time', hour('start_time').alias('hour'), dayofmonth('start_time').alias('day'),\
                           weekofyear('start_time').alias('week'), month('start_time').alias('month'),\
                           year('start_time').alias('year'), dayofweek('start_time').alias('weekday'))
    
    time_table.write.partitionBy(['year', 'month']).parquet(time_path, mode='overwrite')
    time_table.show()
    
    song_df = spark.read.parquet(output_data + 'analytics/songs_table/')
    song_df.show()
    
    
    # Join songs and logs on song title
    df = df.join(song_df, df.song == song_df.title)
    

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.select(col('start_time').alias('start_time'), col('userId').alias('user_id'), 'level', 'song_id',\
                               'artist_id', col('sessionId').alias('session_id'), 'location', col('userAgent').alias('user_agent'),\
                               'year', month('start_time').alias('month')).withColumn('songplay_id',monotonically_increasing_id())
    songplays_table.show()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(['year', 'month']).parquet(songplays_path, mode='overwrite')


def main():
    """
    - Entry point for the ETL pipeline
    - Initiates spark session and begins processing songs and user logs data
    """
    setup_aws()
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-udacity-data-lake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
