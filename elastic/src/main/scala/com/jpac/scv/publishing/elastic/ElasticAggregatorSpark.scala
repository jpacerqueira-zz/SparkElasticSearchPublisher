package com.jpac.scv.publishing.elastic

import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import com.jpac.scv.publishing.elastic.parameters._
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormatter, DateTimeFormat}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by joao.cerqueira on 24/01/2017.
  *
  **/
object ElasticAggregatorSpark extends App {

  val log = LoggerFactory.getLogger(this.getClass.getName)

  override def main(args: Array[String]) {

    val inputDate = ParamDateHdfs(args) match {
      case Some(x) => x
      case _ => throw new IllegalArgumentException("Wrong input parameters")
    }

    val sparkConfig = new SparkConf()
      .setAppName("Gaming SCV - AggregatorSpark v1.0 ")
      .set("spark.hadoop.validateOutputSpecs", "false")
    //.setMaster(cluster)

    val sparkContext = new SparkContext(sparkConfig)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

    executionRawStagedJob(sparkContext, sqlContext, inputDate, 5)

    sparkContext.stop()
  }

  def executionRawStagedJob(sparkContext: SparkContext, sqlContext: SQLContext, inputDate: ParamDateHdfs, inputRepartition: Int): Unit = {

    // Yesterday
    val yesterday = DateTime.now().minusDays(1)

    val eventLogDate = if (inputDate.dthr.length < 10) yesterday else utils.stringTODateLong(inputDate.dthr,"yyyy-MM-dd")

    println("eventLogDate=" + eventLogDate)

    val eventFormatDate = DateTimeFormat.forPattern("yyyy-MM-dd")
    val eventDate = eventLogDate.asInstanceOf[DateTime].toDateTime().toString(eventFormatDate)

    println("eventDate=" + eventDate)

    // Number partitions in Output
    val numRepartition = inputRepartition

    import sqlContext.implicits._

    // Define an udf
    val dateToStr = udf(utils.defaultFormateDateTime)

    val dataRawGigya = "/data/raw/gfans/person/dt=" + eventDate
    val dataStagedGigya = "/data/staged/gfans/person/dt=" + eventDate
    val dataPublishedGigya = "/data/published/gfans/person/dt=" + eventDate

    val dataRawGigyaPath = dataRawGigya
    val dataStagedGigyaPath = dataStagedGigya
    val dataPublishedGigyaPath = dataPublishedGigya

    println("input=" + dataRawGigyaPath)

    val mastyesterdayGigyaDF: DataFrame = sqlContext.read.json(s"${dataRawGigyaPath}/*").toDF()

    mastyesterdayGigyaDF.printSchema()

    val dailyGigya = mastyesterdayGigyaDF
      .filter("dt IS NOT NULL")
      .filter("results IS NOT NULL")
      .select(explode(col("results")).as("gigya"))
      .filter("gigya.UID IS NOT NULL")
      .persist(newLevel = StorageLevel.MEMORY_AND_DISK_2)

    dailyGigya.printSchema()

    val totalData = dailyGigya.count()
    println(s" DAILY GiGYA - TOTAL RECORDS : ${totalData}")

    // save Daily results in paquet format
    val dailyGigyaSave = dailyGigya.repartition(numRepartition).write.mode("overwrite").parquet(s"${dataStagedGigyaPath}")

    // ElasticSearch spark.sql.Df cached with 5 partitions
    val dailyPersonGigyaInEs = sqlContext.read.parquet(s"${dataStagedGigyaPath}")
      .repartition(numRepartition).persist(newLevel = StorageLevel.MEMORY_AND_DISK_2)

    dailyPersonGigyaInEs.printSchema()

    val totalData2 = dailyPersonGigyaInEs.count()
    println(s" DAILY Stage GiGYA - TOTAL RECORDS : ${totalData2}")

    // Persist Data as Parquet quicker to deep search for next Job
    val dailyPersonGigyaInSave = dailyPersonGigyaInEs
      .select('gigya ("UID") as 'GIGYA_UID, 'gigya ("created") as 'CREATED_DATE, 'gigya ("lastLoginTimestamp") as 'LAST_LOGIN_TIMESTAMP, 'gigya ("socialProviders") as 'SOCIAL_PROVIDER)
      .withColumn("LAST_LOGIN_DATE", dateToStr(col("LAST_LOGIN_TIMESTAMP")))
      .write.mode("overwrite").parquet(s"${dataPublishedGigyaPath}")

  }

}