# Project: Data Modeling with Postgres
## Database purpose

Store information about Sparkify music streaming app in order to ease data query (using SQL). 

Using SQL the analysis team will be able to write queries to know more about user preferences.

## How to run the scripts

Assumption : executed from project Workspace

Execute the following commands :

 `run create_tables.py # create DB tables`
 `run etl.py # load DB tables from JSON files`

## Project files

 - **data/song_data** : songs metadata (JSON files)
 - **data/log_data** : music streaming app activity information (JSON files)
 - **create_tables.py** : Python script to create postgreSQL DB tables
 - **etl.ipynb** : reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
 - **etl.py** : reads and processes files from song_data and log_data and loads them into your tables
 - **README.md** : provides project information
 - **sql_queries.py** : contains all SQL statements (table creation, inserts, queries)
 - **test.ipynb** : displays the first few rows of each table to check the database
 
 ## Database schema and ETL pipeline discussion
The implementation uses a **star schema** with : 
 - 1 fact table **songplays** : log data associated with song plays (contains start time, user_id, song_id)
 - Asumption : there is one access possible per user simultaneously. An unique constraint on user_id and start_time has been created. If there might be multiple accesses per user simultaneously, this constraint should be removed
 - 4 dimensions tables : 
 - **users** : app users 
 - **songs** : app songs
 - **artists** : app songs artits
 - **time** :  start_time of fact table songplays broken down into hour, day, week, month, year, weekday

Remarks : 
* CHECK and NOT NULL constraints have been added based on the data found in the data/log_data files. Some on them might be removed if the files dataset would become less constrained or if the performance requires it. 
* FOREIGN KEY could have been added between tables but it could impact performance if the dataset becomes bigger. They would be required if the integrity needs to be enforced

The ETL pipeline (etl.py script) iterates trough data/song_data and data/log_data JSON files and insert into DB tables. 

The **PostgreSQL COPY** protocol is used in order to fasten the load. Dataframes are bulk inserted into the tables. To avoid having constraints error, deduplication is made before bulk insert.

An initial alternative was to write the following inserts : 
 - INSERT INTO users, songs and artists use ON CONFLICT UPDATE clause in order to avoid duplicates and update records with the most recent files information
 - INSERT INTO time uses ON CONFLICT DO NOTHING clause because a timestamp will always be broken down into the same values
 - INSERT INTO songplays uses ON CONFLICT DO NOTHING clause because based on the current dataset, update would not provide more recent information

 ## Sample queries
* What is the percentage of paid songs played ? 
 `SELECT round(100 * sum(CASE WHEN level = 'paid' THEN 1 ELSE 0 END)::decimal / count(1), 2) pct FROM songplays;`
* What are the top 5 active day of week and hour (most songs played) ?
 `SELECT weekday, hour, count(*) FROM songplays s JOIN time t ON s.start_time = t.start_time GROUP BY weekday, hour ORDER BY count(*) DESC LIMIT 5;` 
* Who are the top 5 active user (most songs played paid or not) : 
 `SELECT concat(u.first_name, ' ', u.last_name), count(*) FROM songplays s JOIN users u ON u.user_id = s.user_id GROUP BY u.user_id ORDER BY count(*) DESC LIMIT 5;`