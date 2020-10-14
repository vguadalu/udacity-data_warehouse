# Sparkify Cluster and Database

### Purpose
The purpose of this database is to allow easy analysis of the songs being played by Sparkify users. The data is located in s3 buckets

### Schema
The fact table is called songplays and contains the songplay id, timestamp, user id, artist id, level, song_id, location, session id and user agent. The songplay id is an integer and the primary key for this table. The start time is a NOT NULL timestamp and the foreign key to the times table. The user id, artist id, song id and location are all varchar. The artist id, user id and song id are the foreign keys to the artists, users and songs table,respectively, and can not be null since they are foreign keys.  The level (char(4)) can not be null since the membership level must be part of the user's account info. The session (integer) and is not null because the a session id should be created when a session is started. The location can be null since its not required to be provided. The user agent (varchar) and can not be null.

There are four dimension tables, users, artists, times, and songs. The users table contains the user id, first name, last name, gender and level. 
The user id is an integer and the primary key. The first name and last name are varchar and can not be null since they are required when creating a Sparkify account. The gender is a char(1) and can be null. The level is a char(4) and can not be null since its part of the Sparkify account wether they are paying or using it for free.
The artists table contains the artist id, name, location, latitude and longitude. The artist id is a varchar and the primary key. The name and location are both varchar since its not expected to have arist names and location with more than 50 characters. The name can not be null because for there to be an artist id there must be a name for that artist. The location, latitide (decimal) and longitude(decimal) can be null since its not required information to be provided. 
The songs table contains the song id, title, artist id, year and duration. The song id(varchar is the primary key and thus can not be null and must be unique. The title(varchar) is the name of the song and can not be null since the title should be provided when creating a new song id. The artist_id (varchar), year(integer) and duration(decimal) can be null and is not a required field when inserting data into the songs table. 
The times table contains the start time, hour, day, week, month, year and weekday. The start time is a timestamp and is the primary key of the times table. The hour, day, week, month, year, weekday are all integers and can not be null since they can be extracted from the start time and allow for the data to be more versatile and easily accessible. 

![alt text](https://github.com/vguadalu/udacity-data_warehouse/blob/main/Sparkify%20Star%20Schema.jpeg)
### ETL
The data files for the database are stored in S3 buckets. The data was extracted by placing all the data from the song and log data sets in staging tables. Once all the data was in the songs and logs staging tables, the desired data for each table in the star schema was populated by using an INSERT query from the staging tables.

Loading data from S3 bucket into the staging tables was done using partitioned data to reduce the time it takes.


### Contents of Package
The following are the files contained in this package:
- redshift: Python script which creates the IAM role and redshift cluster that will be used to analyze the Sparkify data.
- create_tables.py: Python script which connects to the created database and creates empty tables using the SQL queries located at sql_queries.py.
- etl.py: Python script which extracts, transforms and inserts the data from all the JSON files located in data/ into the tables of the Sparkify databse.
- README.md: ReadMe currently reading, which contains the purpose and implementation of the Sparkify database, content of the package, and how to generate the database.
- sql_queries.py: Python script which contains the DROP, CREATE, INSER and COPY SQL queries to be used by create_tables.py and etl.py.

### How to create the Sparkify Database
To generate the Sparkify database the following step should be followed:
1. Generate the redshift cluster and iam role
     python redshift.py create
2. Check status of redshift cluster until the cluster becomes available
     python redshift.py check
3. Generate the empty tables:
    python create_tables.py
4. Insert data into the tables:
    python etl.py

Don't forget to clean up cluster when finished using it. The following command will delete the redshift cluster and the IAM role.
    python redshift.py clean
