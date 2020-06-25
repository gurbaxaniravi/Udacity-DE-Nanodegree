# Project: Data Warehouse

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description
In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

# Project Datasets
You'll be working with two datasets that reside in S3. Here are the S3 links for each:

***Song data***: s3://udacity-dend/song_data<br>
***Log data***: s3://udacity-dend/log_data<br>
***Log data json path***: s3://udacity-dend/log_json_path.json

## Song Dataset
![Song Data File](images/song_data.png)

## Log Dataset
![Log Data File](images/event_data.png)

# Project Flow
The flow of the whole project can be summarized as 



![Project Flo](images/Project_Flow.png)

