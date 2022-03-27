# <u>Data Engineer nanodegree / Udacity project : Set up a Cloud Data Warehouse with AWS Redshift</u>
## Table of Contents
1. [Project info](#project-info)
2. [Repository files info](#repository-files-info)
3. [Prerequisite to scripts run](#pre-requisite)
4. [Database modelling](#database-modelling)
5. [How to run the scripts](#how-to-run-the-scripts)

***

### Project info

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud.

Their data resides in AWS S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The purpose of this project is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

The JSON files consist of :
* log files containing information about the streaming activity of users,
* song files containing meta data of the songs.


***
### Repository files info

* `sql_queries.py` contains the sql statements:
    * to drop , create staging tables, dimensional and fact tables in AWS Redshift postgre  database,
    * insert data from S3 buckets containing the JSON files to the staging tables,
    * insert data from the staging tables to the fact and dimensional tables. 
* `create_tables.py` is the script that runs the sql statements in `sql_queries.py` to drop and create the tables. 
* `etl.py` is the script that runs the sql statements in `sql_queries.py` to loads the data from S3 bucket to the stating tables and from the staging table to the fact/dimentional tables. 
* `etl_test.ipynb` is the notebook that is used to test the whole ETL.
* `dwh.cfg` is the configuration file that contains the necessary parameters to set up the Redshift cluster,access the database, access the S3 buckets. It also contains the credentials to access AWS. 
* `initialization.py` is the first script run in order to build the Redshift cluster. 
* `aws_objects_delete.py` is the script to run when you are done with the project. It will delete all the AWS objects created by `initialization.py` script.



***
### Prerequisite to scripts run

* The first step is to create a Redshift cluster and this requires  to create beforehand the credentials of an IAM user necessary to access and use AWS tools and applications.
* Once you have the credentials, they are stored in `dwh.cfg` configuration file under the tags `key` and `secret`.
* Fill the following settings that will be used to create the cluster and the database.
The below values for the cluster settings are suggestions:

>[CLUSTER]
> 
> dwh_cluster_type = multi-node
> 
> dwh_num_nodes = 4
>
> dwh_node_type = dc2.large
>
> dwh_cluster_identifier = dwhCluster
>
> dwh_region = us-west-2
> 
> dwh_iam_role_name = dwhRole
>
> dwh_db = dwh
>
> dwh_db_user = dwhuser
>
> dwh_db_password = Passw0rd
>
> dwh_port = 5439

> [S3_BUCKET]
>
> log_data = 's3://udacity-dend/log-data'
>
> log_jsonpath = 's3://udacity-dend/log_json_path.json'
> 
> song_data = 's3://udacity-dend/song-data'

The settings are relating to the Redshift cluster you want to create and the database credentials you want to use.

Then run the script `initialization.py` to create the cluster and the database. The script will also create an incoming TCP rule to access the cluster. It then updates the configuration file with the following information :
`dwh_endpoint` and `dwh_role_arn` that will be used in any connection to the database. 

***
## Database modelling

The database model consists of :
* one fact table : `songplay`,
* dimension tables : `users`, `songs`, `artists` , `time`. 

The data model is a simple star schema.
The tables were created with constraints, sort and distribution keys.

Mapping rules from JSON files to the tables:

* __songplay__:

| column | source file.field  |
|:--------------|:-------------|
| start_time | *log_file.ts* |
| user_id | *log_file.userid* |
| level | *log_file.level* |
| song_id | *song_file.songid*|
| artist_id | *song_file.artist_id*|
| session_id | *log_file.sessionId*|
| location | *log_file.location*|
| user_agent | *log_file.userAgent*|

* __users__:

| column | source file.field  |
|:--------------|:-------------|
| user_id | *log_file.userid* |
| first_name | *log_file.firstName* |
| last_name | *log_file.lastName* |
| gender | *log_file.gender*|
| level | *log_file.level*|

* __songs__:

| column | source file.field  |
|:--------------|:-------------|
| song_id | *song_file.songid* |
| title | *song_file.title* |
| artist_id | *song_file.artist_id* |
| year | *song_file.year*|
| duration | *song_file.duration*|

* __artists__:

| column | source file.field  |
|:--------------|:-------------|
| artist_id | *song_file.artist_id* |
| name | *song_file.artist_name* |
| location | *song_file.artist_location* |
| latitude | *song_file.artist_latitude*|
| longitude | *song_file.artist_longitude*|

* __time__:

| column | source file.field  |
|:--------------|:-------------|
| start_time | *log_file.ts* |
The other info from the `time` table are derive
from the start_time column.


**<u>Important</u>** : `song_id` and and `artist_id` columns in `songplay` table are derived from the `song_file` JSON file.

***

## How to run the scripts


Once the configuration file is complete, you can use `etl_test.ipynb` to run the ETL process.
It will run the scripts in the following order :
> 1. `initialization.py` will create the cluster and update the configuration file.
> 2. `create_tables.py` will create the tables in the database.
> 3. `etl.py` will insert data in the tables.

The notebook contains queries to check the whole process works fine.
> 4. `aws_objects_delete.py` will delete the cluster and the AWS objects created for the project.




