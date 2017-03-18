package com.jpac.scv.publishing.elastic

import com.cloudera.org.joda.time.DateTimeZone
import com.jpac.scv.publishing.elastic.parameters._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.LoggerFactory

/**
  * Created by joao.cerqueira on 24/01/2017.
  *
  **/
object ElasticSparkPublisher extends App {

  val log = LoggerFactory.getLogger(this.getClass.getName)

  override def main(args: Array[String]) {

    val inputArgs = ParamDateHdfs(args) match {
      case Some(x) => x
      case _ => throw new IllegalArgumentException("Wrong input parameters")
    }

    val esNodes = "localhost"
    val esIndexStage = "dev-"
    val esIndexName = "gfansview"
    val esType = "person"

    // Yesterday
    val yesterday = DateTime.now().minusDays(1)

    val eventLogDate = if (inputArgs.dthr.length < 10) yesterday else utils.stringTODateLong(inputArgs.dthr,"yyyy-MM-dd")

    println("eventLogDate=" + eventLogDate)

    val eventFormatDate1 = DateTimeFormat.forPattern("yyyyMMdd")
    val eventFormatDate2 = DateTimeFormat.forPattern("yyyy-MM-dd")

    val eventIdDate = eventLogDate.asInstanceOf[DateTime].toDateTime().toString(eventFormatDate1)
    val eventDate = eventLogDate.asInstanceOf[DateTime].toDateTime().toString(eventFormatDate2)

    println("eventDate=" + eventDate)

    val esIndexType = s"${esIndexStage}${esIndexName}-${eventIdDate}/${esType}"

    val sparkConfig = new SparkConf().setAppName("Gaming SCV - SparkPublisher v1.0")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("es.index.auto.create", "true")
      .set("es.resource", esIndexType)
      .set("es.nodes", esNodes)
      .set("es.nodes.discovery", "true")

    val sparkContext = new SparkContext(sparkConfig)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

    executionRawStagedJob(sparkContext, sqlContext, esIndexType, eventDate, 5)

    sparkContext.stop()
  }

  def executionRawStagedJob(sparkContext: SparkContext, sqlContext: SQLContext, esIndexType: String,  eventDate: String, inputRepartition: Int): Unit = {

    // Number partitions in Output
    val numRepartition = 5

    val dataPublishedGigya = "/data/published/gfans/person/dt=" + eventDate

    val dataPublishedGigyaPath = dataPublishedGigya

    val persistCurltoEs = sqlContext.read.parquet(s"${dataPublishedGigyaPath}")
      .persist(newLevel = StorageLevel.MEMORY_AND_DISK_2)

    persistCurltoEs.printSchema()

    // Save to ElasticSearch only in elasticsearch sql context
    // ElasticSearch Library functionality need to be cast asInstanceOf[org.elasticseach.spark.sql.SparkDataFrame]
    persistCurltoEs.repartition(numRepartition).saveToEs(esIndexType)

  }

}
