# Processing Big Data Analytics README
@author: Phillip Wang (pw1287), Tianshu Wang(tw2198), M Anas(ma5674)

## Overview

This project aims to reveal about how people interacts with mobile applications, an essential service that is built into our lives and is becoming increasingly involved in them. We aims to approach from this issue using three datasets with different demographics, purposes, and schema; all of which built around the idea of how we use mobile apps daily, in an effort to generalize patterns arised in the datasets.

## Tools Used
* Hadoop MapReduce
* Apache Hive
* Apache Spark

## Frappe
Frappe is a dataset used to develop context-aware mobile app recommender systems, it is the primary dataset used in the paper [*Frappe: Understanding the Usage and Perception of Mobile App Recommendations In-The-Wild*](https://arxiv.org/abs/1505.03014). The dataset itself is available [here](https://www.baltrunas.info/context-aware).

### Schema

Frappe is divided into two tables: *frappe.csv* and *meta.csv*. Below is the schema for *frappe.csv*

| USER_ID | APP_ID | USAGE_COUNT | daytime | weekday | isweekend | home/work | COST | weather | COUNTRY | CITY |
| ------- | ------ | ----------- | ------- | ------- | --------- | --------- | ---- | ------- | ------- | ---- |

Below is the schema for *meta.csv*

| APP_ID | PACKAGE | CATEGORY | DOWNLOADS | DEVELOPER | icon | LANGUAGE | description | NAME | PRICE | RATING | short desc |
| ------- | ------ | ----------- | ------- | ------- | --------- | --------- | ---- | ------- | ------- | ---- | ----|

All the non-capitalized columns are being dropped in the data cleaning step

### Data Loading
To load the data, first head to the link above to download the zip file, unzip the file, and transfer it to the High Performance Cluster. Commands can be found at `data_ingest/pw1287/DatasetSetup.sh`

In order to use Hadoop/Hive/Spark with our dataset, we should first load the files into HDFS. Use the following commands.
```shell=
hdfs dfs -put frappe.csv/meta.csv /hdfs/path/for/storage 
```

Also, since some of the data cleaning and much of the analytics is built around Hive. We should also load the data stored in HDFS into Apache Hive. The commands can be found at `/data_ingest/pw1287/HiveDataLoading.hql` Note that it should be executed after the data cleaning step.

### Data Profiling
We could do a basic data profiling using MapReduce. You can find the Java source code in `/profiling_code/pw1287/CountRecs*.java`. The compiled classes are there as well. To run the MapReduce job, 

```shell=
jar -cvf CountRecs.jar *.class
hadoop jar CountRecs.jar CountRecs /path/to/input.csv /output/storage/path
```
Notice that the output folder should not exist at the time of running the command.

You can see the result by going to the specified output folder on HDFS with `hdfs dfs -cat /output/storage/path/part-r-00000`.

Additionally, by running the commands from `/profling_code/pw1287/profiling.hql`, we could further understand some descriptive stats of the dataset. You can run the hql file using
```shell=
hive -f profiling.hql
```
Hive will compile the SQL commands into MapReduce jobs in realtime and display the results.

### Data Cleaning

#### Frappe.csv
Now that we got a rough idea of the size of the dataset, we can proceed to extract revevant information from the files. We first used the MapReduce to clean the *frappe.csv* because of its large size. The source code (and the three complied classes)  can be found at `/etl_code/pw1287/Cleaning.java`. Use the commands introduced in the previous section to initiate a MapReduce job on the cluster.

#### Meta.csv
Since *meta.csv* contains the meta information about each mobile apps being referenced and is of relatively smaller size, we use Spark to do the cleaning while also obtaining some useful analytics along the way. The code can be found at `/etl_code/pw1287/meta_cleaning.scala`, run the code using the following commands,
```shell=
spark-shell -deploy-mode client -i meta_cleaning.scala
```
It will read the meta table stored in Hive, drop the extraneous columns, produce some statistics, and store the results into meta_cleaned in Hive.

### Analytics
After the data cleaning, we could further obtain analytics on the datasets. Using the hql commands & scala files in `/ana_code/pw1287/`, we could read from the Hive table create Dataframe directly from SQL queries. The results of the scala code will either be displayed on standard output or saved into Hive table. The analytics we obtained using these code include:
1. Number of users by countries
2. Apps with the most users
3. Apps with the most usage counts
4. Most used app by countries
5. Average number of unique apps per user
6. Developers with the most apps
7. Number of apps under each category
8. ...


## istas
istas(https://github.com/aliannejadi/istas) is a cross app mobile search queries dataset. It contains timestamp, userid, query, app used, as well as the app usage sequence history of the user before the submission of the search query. The most crucial part of this dataset is the usage sequence field.

### Schema
| timestamp | UserID | Query | App | AppUsages |
| --------- | ------ | ----- | --- | --------- |

### istas steps

#### cleaning
1. The source file of the dataset is provided as json file, Since python has better library support for json files, I wrote a short python script to convert it to csv file. the script is in /etl_code/tw2198/converter.py
2. then put the csv file into hdfs, detail in this step is in /data_ingestion/tw2198/ingestion
3. then clean the data with the MapReduce program in etl_code/tw2198/ , just run 'compile', then 'start', then 'result' 3 script files in this folder, the result will be put in the same folder as 'cleaned.txt'.
4.  If you want, you can run 'clean' script in the folder to remove compiled file as well as output files in hdfs.

#### profiling
1. the profiling code is in profiling_code/tw2198.
2. like before, to run and get data run 'compile', 'start', then 'result' script.
3. you can designate which version of the dataset (either cleaned or origional) to count line with by changing the input file path in the start script.
4. the origional dataset linecount is in ori.txt, the cleaned dataset count is in cleaned_count.txt

#### Analyzation - app usage sequence
1. this part extracts the app usage sequence from the previous cleaning steps and find the most popular triplets in the sequence.
2. The firs step uses MapReduce, the source code is in /ana_code/tw2198/usage_sequence/step1/code, all the script files are in the step1 folder. It uses a similar workflow: compile->start->result
3. then I put the combined result output/output.txt into hdfs, and create a table in hive. you can find the script of put file, create Table, as well as show top 10 most popular sequence hive script in step2 folder
4. the result is in the step2/result file

#### Analyzation - app usage time
1. this step calculate the average usage time of each app and find the most popular app by usage time.
2. The usage_time has a similar folder structure as the previous part.
3. The usage_time mapreduce code has a similar workflow, to run and get result: compile->start->result
4. the hive script is in step2_hive folder. the result is in step2_hive/result.

## LSApp

https://github.com/aliannejadi/LSApp.git
This is a dataset about the sequence of mobile app usage of users. It records user id, session id, time stamp, app name, aswell as the type of the event recorded.

### Schema

user\_id |	session\_id |	timestamp	| app\_name	| event\_type
-- | -- | -- | -- | --


#### directories
The raw file of the dataset is location in hdfs at : ma5674/hw/hw7/lsapp.tsv

The first Map Reduce cleaning on the code produces and output of all the records of data with the only relevant schema fields of userId and app name to the location: ma5674/project/output11
This is a result of Clean 2 Map reduce job located in ma5674 directory as well. Hive database appusage1 is also located at ma5674/project/output11.

The second Map Reduce cleaning on the code produces and output all the unique apps records and remove apps that the user uses recurringly. this is stored at the location: ma5674/project/output12
This is a result of Clean 3 Map reduce job located in ma5674 directory as well. Hive database unique_appusage is also located at ma5674/project/output12.

The second Map Reduce cleaning on the code produces and output all the unique apps in the sequence of 5 apps that the user uses. this is stored at the location: ma5674/project/output14.
This is a result of Clean 4 Map reduce job located in ma5674 directory as well. Hive database sequence_count is also located at ma5674/project/output14.

#### 
The data could be built from the hive queries attached in the code.
