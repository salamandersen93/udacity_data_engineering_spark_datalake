import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from zipfile import ZipFile
import pandas as pd
import pyspark.sql.functions as F
from datetime import datetime

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['ACCESS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['SECRET']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    #df = spark.read.json('data/local-Songdata/*/*/*/*')
    df = spark.read.json('s3a://udacity-dend/song_data/*/*/*/*')
    
    print('dataframe read in')
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").distinct()
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet('s3://s3sparkify/output/songs_output.parquet')
    print('song table written')
    
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").distinct()
    artists_table.write.mode('overwrite').parquet('s3://s3sparkify/output/artist_output.parquet')
    print('artist table written')
    
def process_log_data(spark, input_data, output_data):
    
    #logdf = spark.read.json('data/log-Testdata/*')
    logdf = spark.read.json('s3a://udacity-dend/log_data/*')
    logdf = logdf.filter(logdf.page == 'NextSong')

    users_table = logdf.selectExpr(['userId', 'firstName', 'lastName', 'gender', 'level', 'ts']).distinct()
    users_table.write.mode('overwrite').parquet('s3://s3sparkify/output/users_output.parquet')

    get_ts = udf(lambda x: datetime.fromtimestamp(x / 1000.0))
    logdf = logdf.withColumn('timestamp', get_ts('ts'))
    
    times_table = logdf.select('timestamp').distinct()
    times_table = times_table.withColumn('hour', F.hour('timestamp'))
    times_table = times_table.withColumn('day', F.dayofmonth('timestamp'))
    times_table = times_table.withColumn('week', F.weekofyear('timestamp'))
    times_table = times_table.withColumn('month', F.month('timestamp'))
    times_table = times_table.withColumn('year', F.year('timestamp'))
    times_table = times_table.withColumn('weekday', F.dayofweek('timestamp'))
    times_table.write.partitionBy("year", "month").mode('overwrite').parquet('s3://s3sparkify/output/times_output.parquet')
    
    song_df = spark.read.json('data/local-Songdata/*/*/*/*')

    # referenced https://knowledge.udacity.com/questions/678149 for help on two below blocks
    logdf = logdf.orderBy('timestamp')
    logdf = logdf.withColumn('songplay_id', F.monotonically_increasing_id())
    song_df.createOrReplaceTempView('songs')
    logdf.createOrReplaceTempView('events')

    songplays_table = spark.sql("""
        SELECT
            e.songplay_id,
            e.timestamp,
            e.userId,
            e.level,
            s.song_id,
            s.artist_id,
            e.sessionId as session_id,
            e.location,
            e.userAgent as user_agent,
            year(e.timestamp) as year,
            month(e.timestamp) as month
        FROM events e
        LEFT JOIN songs s ON
            e.song = s.title AND
            e.artist = s.artist_name AND
            ABS(e.length - s.duration) < 2
    """)
    
    songplays_table.write.mode('overwrite').parquet('s3://s3sparkify/output/songplays_output.parquet', partitionBy = ['year', 'month'])

def main():
    spark = create_spark_session()
    print('created spark session')
    input_data = "s3a://udacity-dend/"
    output_data = "s3://s3sparkify/output/"
    
    print('entering process song data')
    process_song_data(spark, input_data, output_data) 
    print('entering process log data')
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
