
 GFANS Prototype
 
- SparkElasticAggregator ::
   Extracts, flattens and transforms and publishes json data
   as parquet files for spark
  

- hdfs paths :
  -  /data/raw/fgans/perform/dt=yyyymmdd
  -  /data/staged/fgans/perform/dt=yyyymmdd
  -  /data/published/fgans/perform/dt=yyyymmdd
 
  
- SparkElasticAggregator ::
   Reads from published and post into ElasticSearch daily index
. 
