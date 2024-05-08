package com.macquarie.rmg.openpages

object ConfigUtil  extends Serializable {

  import org.apache.spark.sql.SparkSession

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  var sparkSession:SparkSession = null

  var projectName:String = null
  var jobName:String = null
  var batchId:String = null


  var configFullPath:String = null

  def readFromJobConfigFile(): String ={

    var strJobConfig = ""

    val spark = sparkSession

    if(spark == null){
      println("ERROR:sparkSession is null! Please set sparkSession!")
    }
    else {
      val dfJobConfig = spark
        .read
        .text(configFullPath)

      strJobConfig = dfJobConfig
        .collect()
        .map{row =>
          row.getString(0)
        }
        .mkString("\n")
    }

    strJobConfig
  }

}
