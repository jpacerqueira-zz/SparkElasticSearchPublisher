package com.jpac.scv.publishing.elastic

import com.cloudera.org.joda.time.DateTimeZone
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormatter, DateTimeFormat}

import org.slf4j.LoggerFactory

/**
  * Created by joao.cerqueira on 24/01/2017.
  *
  **/
object ElasticSparkPublisher extends App {
  val log = LoggerFactory.getLogger(this.getClass.getName)

  val esNodes="localhost"
  val esIndexStage="dev-"
  val esIndexName="gfansview"
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
    .set("es.index.auto.create", "true")
    .set("es.resource", esIndexType)
    .set("es.nodes", esNodes)
    .set("es.nodes.discovery", "true")
    //.setMaster(cluster)

  val sparkContext = new SparkContext(sparkConfig)
  val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

  import sqlContext.implicits._

  // Define an udf
  val dateToStr = udf(utils.defaultFormateDateTime)

  log.info(s"Processing Published Single Person of Gigya as ElasticSearch index  ${eventLogDate}")

  val dataPublishedGigya = "/data/published/gfans/gigya/person/dt="+eventDate

  println("cluster="+cluster)

  val dataPublishedGigyaPath = dataPublishedGigya


  val persistCurltoEs = sqlContext.read.parquet(s"${dataPublishedGigyaPath}")
    .persist(newLevel=StorageLevel.MEMORY_AND_DISK_2)
  
  persistCurltoEs.printSchema()

  // Save to Elastic Search work only in elasticsearch sql context
  persistCurltoEs.repartition(numRepartition).saveToEs(esIndexType)

  sparkContext.stop()

}
