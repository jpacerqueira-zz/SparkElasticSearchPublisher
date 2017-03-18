package com.jpac.scv.publishing.elastic.parameters

/**
  * Created by jpacerqueira on 18/03/2017.
  */
case class ParamDateHdfs(dthr:String = null)

object ParamDateHdfs {

  def apply(args: Array[String]): Option[ParamDateHdfs] = {

    val parser = new scopt.OptionParser[ParamDateHdfs]("ElasticAggregatorSpark") {

      opt[String]("dthr")
        .action((x, c) => c.copy(dthr = x))
        .required()
        .text("Date Input HDFS directory")

    }

    parser.parse(args, ParamDateHdfs())
  }
}