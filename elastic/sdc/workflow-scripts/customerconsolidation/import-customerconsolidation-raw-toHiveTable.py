#
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
#
#SparkContext.setSystemProperty("hive.metastore.uris", "thrift://bigdatalite.localdomain:9083")
#
#sparkSession = (SparkSession
#                .builder
#                .appName('ImportData-TableAs-CustomerConsolidation')
#                .enableHiveSupport()
#                .getOrCreate())
#
# sc is an existing SparkContext.

#### OLD SPARK 1.6 Notation
##
conf = SparkConf()
conf.setAppName('ImportData-TableAs-CustomerConsolidation') 
#
from pyspark import SparkContext
sc = SparkContext()
SparkContext.setSystemProperty("hive.metastore.uris", "thrift://bigdatalite.localdomain:9083")
#
sqlContext = SQLContext(sc)
#
sparkSession =HiveContext(sc)
##
#
#
# Arguments
#
import argparse
## Parse date_of execution
parser = argparse.ArgumentParser()
parser.add_argument("--datev1", help="Execution Date")
parser.add_argument("--datev2", help="Execution Date")
args = parser.parse_args()
if args.datev1:
    processdate = args.datev1
if args.datev2:
    processinputdate = args.datev2
#    
#    
# GENERAL PREPARATION SCRIPT
#  Date in format YYYYMMDD
process_date = processdate
if not process_date:
    process_date = "20190225"
#
# process_date='20190225'
#
### Forced Input TBA ### From Clean Trustable source
process_input_date = processinputdate
if not process_input_date:
    process_input_date = "2019-02-24-*"
#
#process_input_date='2019-02-24-*'
#
#
input_file="hdfs:///data/raw/customerconsolidation/customer/"+process_input_date
#
#
# SparkSQL read files as temp_table 
sql_read_files ="SELECT CAST(customer_raw_files.CUST_ID AS STRING) AS cust_id, CAST(customer_raw_files.FIRST_NAME AS STRING) AS first_name , CAST(customer_raw_files.LAST_NAME AS STRING) AS last_name,"+\
" CAST(customer_raw_files.STREET_ADDRESS AS STRING) AS street_address, CAST(customer_raw_files.POSTAL_CODE AS STRING) AS postal_code, CAST(customer_raw_files.CITY_ID AS STRING) AS city_id , "+\
" CAST(customer_raw_files.CITY AS STRING) AS city, CAST(customer_raw_files.STATE_PROVINCE_ID AS STRING) as state_province_id , CAST(customer_raw_files.STATE_PROVINCE AS STRING) AS state_province , "+\
" CAST(customer_raw_files.COUNTRY_ID AS STRING) AS country_id, CAST(customer_raw_files.COUNTRY AS STRING) AS country, CAST(customer_raw_files.CONTINENT_ID AS STRING) continent_id ,"+\
" CAST(customer_raw_files.CONTINENT AS STRING) AS continent, CAST(customer_raw_files.AGE AS STRING) AS age, CAST(customer_raw_files.COMMUTE_DISTANCE AS STRING) AS commute_distance , "+\
" CAST(customer_raw_files.CREDIT_BALANCE AS STRING) AS credit_balance , CAST(customer_raw_files.EDUCATION AS STRING) AS education, CAST(customer_raw_files.EMAIL AS STRING) AS email, "+\
" CAST(customer_raw_files.FULL_TIME as STRING) AS full_time, CAST(customer_raw_files.GENDER AS STRING) AS gender,CAST(customer_raw_files.HOUSEHOLD_SIZE As STRING) AS household_size, "+\
" CAST(customer_raw_files.INCOME AS STRING) AS income, CAST(customer_raw_files.INCOME_LEVEL AS STRING) AS income_level, CAST(customer_raw_files.INSUFF_FUNDS_INCIDENTS AS STRING) AS insuff_funds_incidents, "+\
" CAST(customer_raw_files.JOB_TYPE AS STRING) AS job_type, CAST(customer_raw_files.LATE_MORT_RENT_PMTS AS STRING) AS late_mort_rent_pmts, CAST(customer_raw_files.MARITAL_STATUS AS STRING) AS marital_status, "+\
" CAST(customer_raw_files.MORTGAGE_AMT AS STRING) AS mortgage_amt, CAST(customer_raw_files.NUM_CARS AS STRING) AS num_cars, CAST(customer_raw_files.NUM_MORTGAGES AS STRING) AS num_mortgages, "+\
" CAST(customer_raw_files.PET AS STRING) AS pet, CAST(customer_raw_files.PROMOTION_RESPONSE AS STRING) AS promotion_response, CAST(customer_raw_files.RENT_OWN AS STRING) AS rent_own, "+\
" CAST(customer_raw_files.SEG AS STRING) AS seg, CAST(customer_raw_files.WORK_EXPERIENCE AS STRING) AS work_experience, CAST(customer_raw_files.YRS_CURRENT_EMPLOYER AS STRING) AS yrs_current_employer, "+\
" CAST(customer_raw_files.YRS_CUSTOMER AS STRING) AS yrs_customer, CAST(customer_raw_files.YRS_RESIDENCE AS STRING) AS yrs_residence, CAST(customer_raw_files.COUNTRY_CODE AS STRING) As country_code, "+\
" CAST(customer_raw_files.USERNAME AS STRING) AS username  FROM customer_raw_files"
# HiveQL Scoring table Load
sql_publish = "INSERT INTO TABLE  customerconsolidation.customer PARTITION (dt="+process_date+")"+\
" SELECT cust_id, first_name, last_name, street_address , postal_code, city_id, city, state_province_id, state_province, country_id, country, continent_id, continent, age, commute_distance, credit_balance, education, email, full_time, gender, household_size, income, income_level, insuff_funds_incidents, job_type, late_mort_rent_pmts, marital_status, mortgage_amt, num_cars, num_mortgages, pet, promotion_response, rent_own, seg, work_experience, yrs_current_employer, yrs_customer, yrs_residence, country_code, username FROM customer_raw"
#
# Spark read Source Files
#
url_toprd_df= sparkSession.read.json(input_file)
url_toprd_df.printSchema()
url_toprd_df.registerTempTable("customer_raw_files")
#
#
####
###sparkSession.sql("ALTER TABLE customerconsolidation.customer DROP IF EXISTS PARTITION (dt="+process_date+")")
#
# Read Cast Variables
df_cast_structure=sparkSession.sql(sql_read_files)
df_cast_structure.printSchema()
df_cast_structure.registerTempTable("customer_raw")
#
# Write into Hive
df_load = sparkSession.sql(sql_publish)
#
sc.stop()
