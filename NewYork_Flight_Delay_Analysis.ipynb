# python data download and sort script (extraction)

import os
import requests
from bs4 import BeautifulSoup
import zipfile
import shutil

class FileDownloader:
def __init__(self, base_url, download_folder):
self.base_url = base_url
self.download_folder = download_folder

def download_file(self, url):

local_filename = os.path.join(self.download_folder, url.split('/')[-1])
with requests.get(url, stream=True) as r:
r.raise_for_status()
with open(local_filename, 'wb') as f:
for chunk in r.iter_content(chunk_size=8192):
f.write(chunk)
return local_filename

def scrape_and_download(self):
os.makedirs(self.download_folder, exist_ok=True)
response = requests.get(self.base_url)
soup = BeautifulSoup(response.content, 'html.parser')
links = soup.find_all('a', href=True)

for link in links:
url = link['href']
if url.endswith('.zip'):
full_url = self.base_url.rstrip('/') + '/' + url.lstrip('/')
full_url = full_url.replace('/PREZIP/PREZIP/', '/PREZIP/')
self.download_file(full_url)

def unzip_files(self):
for item in os.listdir(self.download_folder):
if item.endswith('.zip'):
file_path = os.path.join(self.download_folder, item)
with zipfile.ZipFile(file_path, 'r') as zip_ref:
zip_ref.extractall(self.download_folder)
os.remove(file_path)

def sort_files(self):
for item in os.listdir(self.download_folder):

item_path = os.path.join(self.download_folder, item)
if os.path.isfile(item_path):
suffix = '_'.join(item.split('_')[1:])
suffix_folder = os.path.join(self.download_folder, suffix)
os.makedirs(suffix_folder, exist_ok=True)
shutil.move(item_path, os.path.join(suffix_folder, item))

def execute(self):
self.scrape_and_download()
self.unzip_files()
self.sort_files()

# Configuration
base_url = 'https://www.transtats.bts.gov/PREZIP/'
download_folder = './downloads'

# Execution
downloader = FileDownloader(base_url, download_folder)
downloader.execute()

# putting data in HDFS (loading)
hdfs dfs -put on_time_performance /user/maria_dev/admp_data/
hdfs dfs -put airline_delay_cause.csv /user/maria_dev/admp_data/
hdfs dfs -put airlines_data.csv /user/maria_dev/admp_data/
hdfs dfs -put airports_data.csv /user/maria_dev/admp_data/
hdfs dfs -put cancellation_causes.csv /user/maria_dev/admp_data/
hdfs dfs -put date_data.csv /user/maria_dev/admp_data/

hdfs dfs -chmod 777 /user/maria_dev/admp_data/on_time_performance/
hdfs dfs -chmod 777 /user/maria_dev/admp_data/airline_delay_cause.csv

hdfs dfs -chmod 777 /user/maria_dev/admp_data/airlines_data.csv
hdfs dfs -chmod 777 /user/maria_dev/admp_data/airports_data.csv
hdfs dfs -chmod 777 /user/maria_dev/admp_data/cancellation_causes.csv
hdfs dfs -chmod 777 /user/maria_dev/admp_data/date_data.csv


# initiate spark session with R in CLI (Putty)
sparkR

# read data into dataframes
df_delay_causes <- read.df("/user/maria_dev/admp_data/airline_delay_cause.csv", source = "csv",
header = "true")

printSchema(df_delay_causes)
# showDF(df_delay_causes)

# data pre-processing and cleaning
numeric_columns <- c("year", "month", "arr_flights", "arr_del15",

"carrier_ct", "weather_ct", "nas_ct", "security_ct",
"late_aircraft", "arr_cancelled", "arr_diverted", "arr_delay",
"carrier_delay", "weather_delay", "nas_delay",

"security_delay",

"late_aircraft_delay")

# convert specified columns to integer type
for (col in numeric_columns) {
df_delay_causes <- df_delay_causes %>% withColumn(col, cast(df_delay_causes[[col]], "integer"))
}

# handling missing values
for (col in numeric_columns) {

df_delay_causes <- df_delay_causes %>% na.fill(0, numeric_columns)
}

# removing duplicate rows if any
df_delay_causes <- dropDuplicates(df_delay_causes)

printSchema(df_delay_causes)
# showDF(df_delay_causes)

createOrReplaceTempView(df_delay_causes, "raw_airline_delays")

df_cancellation_data <- read.df("/user/maria_dev/admp_data/on_time_performance", source =
"csv", header = "true")

printSchema(df_cancellation_data)
# showDF(df_cancellation_data)


# data pre-processing and cleaning

numeric_columns <- c("year", "quarter", "month", "dayofmonth", "dayofweek",
"dot_id_marketing_airline", "flight_number_marketing_airline",
"dot_id_originally_scheduled_code_share_airline",
"flight_num_originally_scheduled_code_share_airline",
"dot_id_operating_airline", "flight_number_operating_airline",
"originairportid", "originairportseqid", "origincitymarketid",
"originwac", "destairportid", "destairportseqid", "destcitymarketid",
"destwac", "crsdeptime", "deptime", "depdelay", "depdelayminutes",
"depdel15", "departuredelaygroups", "taxiout", "wheelsoff", "wheelson",
"taxiin", "crsarrtime", "arrtime", "arrdelay", "arrdelayminutes",
"arrdel15", "arrivaldelaygroups", "cancelled", "diverted",
"crselapsedtime", "actualelapsedtime", "airtime", "flights",

"distance", "distancegroup", "carrierdelay", "weatherdelay",
"nasdelay", "securitydelay", "lateaircraftdelay", "firstdeptime",
"totaladdgtime", "longestaddgtime", "divairportlandings",
"divreacheddest", "divactualelapsedtime", "divarrdelay",
"divdistance", "div1airportid", "div1airportseqid", "div1wheelson",
"div1totalgtime", "div1longestgtime", "div1wheelsoff",
"div2airportid", "div2airportseqid", "div2wheelson",
"div2totalgtime", "div2longestgtime", "div2wheelsoff",
"div3airportid", "div3airportseqid", "div3wheelson",
"div3totalgtime", "div3longestgtime", "div3wheelsoff",
"div4airportid", "div4airportseqid", "div4wheelson",
"div4totalgtime", "div4longestgtime", "div4wheelsoff",
"div5airportid", "div5airportseqid", "div5wheelson",
"div5totalgtime", "div5longestgtime", "div5wheelsoff", "duplicate")

# convert specified columns to integer type
for (col in numeric_columns) {
df_cancellation_data <- df_cancellation_data %>% withColumn(col,
cast(df_cancellation_data[[col]], "integer"))
}

# handling missing values
for (col in numeric_columns) {
df_cancellation_data <- df_cancellation_data %>% na.fill(0, numeric_columns)
}

# removing duplicate rows if any
df_cancellation_data <- dropDuplicates(df_cancellation_data)

printSchema(df_cancellation_data)
# showDF(df_cancellation_data)

createOrReplaceTempView(df_cancellation_data, "raw_on_time_performance")

df_cancellation_master <- read.df("/user/maria_dev/admp_data/cancellation_causes.csv", source =
"csv", header = "true")

createOrReplaceTempView(df_cancellation_master, "dim_cancel_causes")


# queries for business questions (transformation)

# BQ 1, 2 and 5
quest125 <- sql("select year, month, airport, carrier, carrier_name, sum(arr_flights) as total_flights, 
sum(arr_del15) as delayed_flights, avg(arr_delay) as avg_arr_delay, sum(arr_delay) as total_delay 
from raw_airline_delays where airport in ('JFK', 'LGA', 'EWR') group by year, month, airport, carrier, 
carrier_name")
showDF(quest125, numRows = 10, truncate = FALSE)
# BQ 3
quest3 <- sql("select year, month, airport, carrier, carrier_name, sum(carrier_ct) as carrier_delay, 
sum(weather_ct) as weather_delay, sum(nas_ct) as nas_delay, sum(security_ct) as security_delay, 
sum(late_aircraft_ct) as late_aircraft_delay from raw_airline_delays where airport in ('JFK', 'LGA', 
'EWR') and year between 2019 and 2023 group by year, month, airport, carrier, carrier_name")
showDF(quest3, numRows = 10, truncate = FALSE)
# BQ 4
quest4 <- sql("select year, month, dest, cancellationcode, reason as cancellation_reason, 
count(cancelled) as num_cancellations from raw_on_time_performance a join dim_cancel_causes b 
on a.cancellationcode = b.code where cancelled = 1 and dest in ('JFK', 'LGA', 'EWR') and year 
between 2019 and 2023 group by year, month, dest, cancellationcode, reason")
showDF(quest4, numRows = 10, truncate = FALSE)


# data validation

createOrReplaceTempView(quest125, " quest125")
createOrReplaceTempView(quest3, " quest3")
createOrReplaceTempView(quest4, " quest4")
showDF(sql(“select year, sum(total_flights) from quest125 group by year order by year”))
showDF(sql(“select year, sum(delayed_flights) from quest125 group by year order by year”))
showDF(sql(“select year, sum(total_delay) from quest125 group by year order by year”))
showDF(sql(“select year, sum(carrier_delay) from quest3 group by year order by year”))
showDF(sql(“select year, sum(weather_delay) from quest3 group by year order by year”))
showDF(sql(“select year, sum(nas_delay) from quest3 group by year order by year”))
showDF(sql(“select year, sum(security_delay) from quest3 group by year order by year”))
showDF(sql(“select year, sum(late_aircraft_delay) from quest3 group by year order by year”))
showDF(sql(“select year, sum(num_cancellations) from quest4 group by year order by year”))

# writing transformed data

single_partition_df <- coalesce(quest125, 1)
write.df(single_partition_df, path = "/user/maria_dev/admp_data/output/quest125_data/", source = 
"csv", mode = "overwrite", header = "true")

single_partition_df <- coalesce(quest3, 1)
write.df(single_partition_df, path = "/user/maria_dev/admp_data/output/quest3_data/", source = 
"csv", mode = "overwrite", header = "true")

single_partition_df <- coalesce(quest4, 1)
write.df(single_partition_df, path = "/user/maria_dev/admp_data/output/quest4_data/", source = 
"csv", mode = "overwrite", header = "true")

# creating hive tables on datasets

create table raw_ontime_performance(
 year string, 
 quarter string, 
 month string, 
 dayofmonth string, 
 dayofweek string, 
 flightdate string, 
 marketing_airline_network string, 
 operated_or_branded_code_share_partners string, 
 dot_id_marketing_airline string, 
 iata_code_marketing_airline string, 
 flight_number_marketing_airline string, 
 originally_scheduled_code_share_airline string, 
 dot_id_originally_scheduled_code_share_airline string, 
 iata_code_originally_scheduled_code_share_airline string, 
 flight_num_originally_scheduled_code_share_airline string, 
 operating_airline string, 
 dot_id_operating_airline string, 
 iata_code_operating_airline string, 
 tail_number string, 
 flight_number_operating_airline string, 
 originairportid string, 
 originairportseqid string, 
 origincitymarketid string, 
 origin string, 
 origincityname string, 
 originstate string, 
 originstatefips string, 
 originstatename string, 
 originwac string, 
 destairportid string, 
 destairportseqid string, 
 destcitymarketid string, 
 dest string, 
 destcityname string, 
 deststate string, 
 deststatefips string, 
 deststatename string, 
 destwac string, 
 crsdeptime string, 
 deptime string, 
 depdelay string, 
 depdelayminutes string, 
 depdel15 string, 
 departuredelaygroups string, 
 deptimeblk string, 
 taxiout string, 
 wheelsoff string, 
 wheelson string, 
 taxiin string, 
 crsarrtime string, 
 arrtime string, 
 arrdelay string, 
 arrdelayminutes string, 
 arrdel15 string, 
 arrivaldelaygroups string, 
 arrtimeblk string, 
 cancelled string, 
 cancellationcode string, 
 diverted string, 
 crselapsedtime string, 
 actualelapsedtime string, 
 airtime string, 
 flights string, 
 distance string, 
 distancegroup string, 
 carrierdelay string, 
 weatherdelay string, 
 nasdelay string, 
 securitydelay string, 
 lateaircraftdelay string, 
 firstdeptime string, 
 totaladdgtime string, 
 longestaddgtime string, 
 divairportlandings string, 
 divreacheddest string, 
 divactualelapsedtime string, 
 divarrdelay string, 
 divdistance string, 
 div1airport string, 
 div1airportid string, 
 div1airportseqid string, 
 div1wheelson string, 
 div1totalgtime string, 
 div1longestgtime string, 
 div1wheelsoff string, 
 div1tailnum string, 
 div2airport string, 
 div2airportid string, 
 div2airportseqid string, 
 div2wheelson string, 
 div2totalgtime string, 
 div2longestgtime string, 
 div2wheelsoff string, 
 div2tailnum string, 
 div3airport string, 
 div3airportid string, 
 div3airportseqid string, 
 div3wheelson string, 
 div3totalgtime string, 
 div3longestgtime string, 
 div3wheelsoff string, 
 div3tailnum string, 
 div4airport string, 
 div4airportid string, 
 div4airportseqid string, 
 div4wheelson string, 
 div4totalgtime string, 
 div4longestgtime string, 
 div4wheelsoff string, 
 div4tailnum string, 
 div5airport string, 
 div5airportid string, 
 div5airportseqid string, 
 div5wheelson string, 
 div5totalgtime string, 
 div5longestgtime string, 
 div5wheelsoff string, 
 div5tailnum string, 
 duplicate string
 ) 

ROW FORMAT DELIMITED FIELDS TERMINATED BY "," 
STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/maria_dev/admp_data/on_time_performance/' OVERWRITE INTO TABLE 
raw_ontime_performance;

create table raw_airline_delays( 
 year string, 
 month string, 
 carrier string, 
 carrier_name string, 
 airport string, 
 airport_name string, 
 arr_flights string, 
 arr_del15 string, 
 carrier_ct string, 
 weather_ct string, 
 nas_ct string, 
 security_ct string, 
 late_aircraft_ct string, 
 arr_cancelled string, 
 arr_diverted string, 
 arr_delay string, 
 carrier_delay string, 
 weather_delay string,
 nas_delay string,
 security_delay string,
 late_aircraft_delay string
) 

ROW FORMAT DELIMITED FIELDS TERMINATED BY "," 
STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/maria_dev/admp_data/airline_delay_cause.csv' OVERWRITE INTO TABLE 
raw_airline_delays;

CREATE TABLE dim_airport_master( 
 airport_code STRING, 
 airport_name STRING, 
 city_name STRING, 
 country_name STRING, 
 country_code STRING
 ) 

ROW FORMAT DELIMITED FIELDS TERMINATED BY "," 
STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/maria_dev/admp_data/airports_data.csv' OVERWRITE INTO TABLE 
dim_airport_master;

CREATE TABLE dim_date_master( 
 date_id STRING,
 event_date STRING,
 year STRING, 
 quarter STRING, 
 month STRING
 ) 

ROW FORMAT DELIMITED FIELDS TERMINATED BY "," 
STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/maria_dev/admp_data/date_data.csv' OVERWRITE INTO TABLE 
dim_date_master;

CREATE TABLE dim_carrier_master( 
 carrier_code STRING, 
 carrier_name STRING
 ) 

ROW FORMAT DELIMITED FIELDS TERMINATED BY "," 
STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/maria_dev/admp_data/airlines_data.csv' OVERWRITE INTO TABLE 
dim_carrier_master;

CREATE TABLE dim_cancellation_master( 
 cancellation_code STRING, 
 cancellation_reason STRING
 ) 

ROW FORMAT DELIMITED FIELDS TERMINATED BY "," 
STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/maria_dev/admp_data/cancellation_causes.csv' OVERWRITE INTO TABLE 
dim_cancellation_master;

CREATE TABLE fact_bq125( 
 year STRING, 
 month STRING, 
 airport STRING,
 carrier STRING, 
 carrier_name STRING, 
 total_flights STRING,
 delayed_flights STRING, 
 avg_arr_delay STRING, 
 total_delay STRING
 ) 

ROW FORMAT DELIMITED FIELDS TERMINATED BY "," 
STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/maria_dev/admp_data/output/quest125_data/' OVERWRITE INTO TABLE 
fact_bq125;

CREATE TABLE fact_bq3( 
 year STRING, 
 month STRING, 
 airport STRING,
 carrier STRING, 
 carrier_name STRING, 
 carrier_delay STRING, 
 weather_delay STRING, 
 nas_delay STRING,
 security_delay STRING, 
 late_aircraft_delay STRING
 ) 

ROW FORMAT DELIMITED FIELDS TERMINATED BY "," 
STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/maria_dev/admp_data/output/quest3_data/' OVERWRITE INTO TABLE 
fact_bq3;

CREATE TABLE fact_bq4( 
 year STRING, 
 month STRING, 
 dest STRING,
 cancellationcode STRING, 
 cancellation_reason STRING,
 num_cancellations STRING
 ) 

ROW FORMAT DELIMITED FIELDS TERMINATED BY "," 
STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/maria_dev/admp_data/output/quest4_data' OVERWRITE INTO TABLE 
fact_bq4;
