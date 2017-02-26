
 GFANS Prototype
 
- SparkElasticAggregator ::
   Extracts, flattens and transforms json data
   To indexable published parquet files
  

- hdfs paths :
    /data/raw/fgans/perform/dt=0
    /data/staged/fgans/perform/dt=0
    /data/published/fgans/perform/dt=0
 
  
- SparkElasticAggregator ::
   Reads from published and post into ElasticSearch daily index
. 
