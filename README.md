## Project : Data Warehouse
### This Project is the third Project for Udacity Data Engineering Nanodegree Program.
#### Project Description 
## In this project, we'll apply what we've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.


# Data Source 
The source data is in log files given the Amazon s3 bucket  at `s3://udacity-dend/log_data` 
and `s3://udacity-dend/song_data` containing log data and songs data respectively.

Log files contains songplay events of the users in json format 
while song_data contains list of songs details.

## Dimension Table
 Users , Songs , Artists, Time
 
## Fact table
Songplay

# To run the Project
update the `dwh.cfg` with Amazon redshift cluster credentials and IAM role that can access the cluster
Run `python create_tables.py`. This will create the database and all the required tables.
Run `python etl.py`. This will start pipeline which will read the data from files and populate the tables.
