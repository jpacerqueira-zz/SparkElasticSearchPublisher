package ptv.gaming.scv.publishing.elastic

import com.cloudera.org.joda.time.DateTimeZone
import com.databricks.spark.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark

import org.apache.spark.launcher.SparkLauncher
import org.joda.time.Period
import org.joda.time.format.DateTimeFormat

import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormatter, DateTimeFormat}

import org.slf4j.LoggerFactory

/**
  * Created by joao.cerqueira on 24/01/2017.
  *
  **/
object ElasticAggregatorSpark extends App {
  val log = LoggerFactory.getLogger(this.getClass.getName)

  val esNodes="localhost"
  val esIndexStage="dev-"
  val esIndexName="persondailyview"
  val esType="person"

  // Yesterday
  val yesterday= DateTime.now().minusDays(1)

  // DateTimeFormat.forPattern(dtFormat).withZone(DateTimeZone.forID(timezone))
  val eventLogDate = yesterday

  println("eventLogDate="+eventLogDate)

  val eventFormatDate1 = DateTimeFormat.forPattern("yyyyMMdd")
  val eventFormatDate2 = DateTimeFormat.forPattern("yyyy-MM-dd")

  val eventIdDate = eventLogDate.toDateTime().toString(eventFormatDate1)
  val eventDate = eventLogDate.toDateTime().toString(eventFormatDate2)

  println("eventDate="+eventDate)

  val esIndexType = s"${esIndexStage}${esIndexName}-${eventIdDate}/${esType}"


  // Number partitions in Output
  val numRepartition = 5

  // Cluster mAster types
  // val cluster = "local" // -> Laptop
     val cluster = "yarn-client" // -> package
  // val cluster = "master" // -> Ozzie

  val sparkConfig = new SparkConf().setAppName("Gaming SCV - Aggregator ")
    .set("spark.hadoop.validateOutputSpecs", "false")
    .set("spark.default.parallelism", "100")
    .set("es.index.auto.create", "true")
    .set("es.resource", esIndexType)
    .set("es.nodes", esNodes)
    .set("es.nodes.discovery", "true")
    //.setMaster(cluster)

  val sparkContext = new SparkContext(sparkConfig)
  val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
  import sqlContext.implicits._

  log.info(s"Processing Single Customer Viewing : Person Logs calculation and Loading  ${eventLogDate}")


  val dataRawPerson = "/data/raw/person/daily/dt="+eventDate
  val dataStagedPerson = "/data/staged/person/daily/dt="+eventDate
  val dataPublishedPerson = "/data/published/person/daily/dt="+eventDate

  println("cluster="+cluster)

  //val dataRawPersonPath:String = if (!(cluster.equals("local")))  dataRawPerson else "~"+dataRawPerson
  //val dataStagedPathPath:String = if (!(cluster.equals("local"))) dataStagedPerson else "~"+dataStagedPerson
  //val dataPublishedPersonPath:String = if (!(cluster.equals("local"))) dataPublishedPerson else "~"+dataStagedPerson

  val dataRawPersonPath = dataRawPerson
  val dataStagedPersonPath = dataStagedPerson
  val dataPublishedPersonPath = dataPublishedPerson

  println("input="+dataRawPersonPath)

  val mastyesterdayPersonDF :DataFrame = sqlContext.read.json(s"${dataRawPersonPath}/*").toDF()

  mastyesterdayPersonDF.printSchema()

  val dailyPerson = mastyesterdayPersonDF
    .filter("dt IS NOT NULL")
      .filter("results IS NOT NULL")
      .select(explode(col("results")).as("person"))
    .persist(StorageLevel.MEMORY_AND_DISK)

  dailyPerson.printSchema()

  val totalData = dailyPerson.count()
  println(s" DAILY Persons - TOTAL RECORDS : ${totalData}")

  // save Daily results in paquet format
  dailyPerson.repartition(numRepartition).write.mode("overwrite").parquet(s"${dataStagedPersonPath}")

  // ElasticSearch spark.sql.Df cached with 5 partitions
  val dailyPersonInEs = sqlContext.read.parquet(s"${dataStagedPersonPath}").repartition(numRepartition).cache()

  dailyPersonInEs.printSchema()

  // ElasticSearch Library functional need to be cast asInstanceOf[org.elasticseach.spark.sql.SparkDataFrame]
  dailyPersonInEs
      .select('person("UID") as 'PERSON_UID, 'person("createdTimestamp") as 'CREATED_DATE, 'person("lastLogin") as 'LAST_LOGIN_DATE, 'person("socialProviders") as 'SOCIAL_PROVIDER)
      .write.mode("overwrite").parquet(s"${dataPublishedGigyaPath}")


   val saveCurltoEs = sqlContext.read.parquet(s"${dataPublishedPersonPath}")
     .repartition(numRepartition).toJavaRDD.persist(StorageLevel.MEMORY_AND_DISK_2)

  // Save to Elastic Search work only in elasticsearch sql context
  val saveCurltoEs = sqlContext.read.parquet(s"${dataPublishedPersonPath}").repartition(numRepartition).toDf().asInstanceOf[org.elasticseach.spark.sql.SparkDataFrame].
  .saveToEs(esIndexType).asInstanceOf[org.apache.spark.sql.DataFrame => org.elasticsearch.spark.sql.SparkDataFrameFunctions]

  // Works with JavaRDD context 
  EsSpark.saveToEs(saveCurltoEs.toJavaRDD,esIndexType) //.asInstanceOf[org.apache.spark.sql.DataFrame => org.elasticsearch.spark.sql.SparkDataFrameFunctions]

  dailyPerson.unpersist()

  sparkContext.stop()

}
