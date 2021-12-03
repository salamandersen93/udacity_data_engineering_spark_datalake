<h1> Sparkify - Project 4: ETL Using Spark </h1>

<h2> Overview: </h2>

The purpose of this project is to design an ETL pipeline using Python and Spark to solve a business challenge for a hypothetical music streaming startup called Sparkify. The business challenge is a need to transition from a data warehouse to a data lake to accommodate a growing userbase and song database. Currently, their data exists in an S3 bucket as a directory of JSON logs of user activity and a separate directory of JSON metadata containing information about songs hosted on their applicationn. The task to be executed in this project is employ Spark to build an ETL pipeline to extract the data from S3, process using Spark, and reload the data back into an S3 as a set of dimensional tables.

<h2> Approach Justification: </h2>

Source data from the S3 data warehouse will be extracted and transformed using Spark and SparkSQL. The song dataset will be transformed into two independent tables - the songs table and the artist table.

<b> Song Table Schema: </b> songid, title, artistid, year, duration
<b> Artist Table Schema: </b> artistid, name, location, latitude, longitude

Data from the S3 log dataset will be transformed into three additional tables - the users table, time table, and songplays table.

<b> User Table Schema: </b> userid, firstname, lastname, gender, level
<b> Time Table Schema: </b> starttime, hour, day, week, month, year, weekday
<b> Songplay Table Schema: </b> songplayid, starttime, userid, level, songid, artistid, sessionid, location, useragent 

The above tables follow the star schema, with the songplay table reprsenting the fact table and the remaining four tables comprising the dimension tables. This schema allows optimization for analytical queries on song play data.
