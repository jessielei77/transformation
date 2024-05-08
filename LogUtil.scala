package com.macquarie.rmg.openpages

object LogUtil extends Serializable {

  import org.apache.spark.sql.SparkSession
  import org.joda.time.DateTime
  import com.macquarie.rmg.openpages.Transformation.jobConfig

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  var sparkSession:SparkSession = null

  var projectName:String = null
  var jobName:String = null
  var batchId = 0L

  var logFullPath:String = null
  var logTableFullName:String = null

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


  def getCurrentTimeAsString(): String ={
    val strNow = DateTime.now().toString("yyyy-MM-dd HH:mm:ss")

    strNow
  }


  def info(
            step:String,
            msg:String
          ): Unit ={
    val level = "INFO"
    val strNow = getCurrentTimeAsString()

    appendLog_Parquet(
      strNow,
      level,
      step,
      msg
    )

    println(strNow + " " + level + ":" + msg)
  }

  def warn(
            step:String,
            msg:String
          ): Unit ={
    val level = "WARN"
    val strNow = getCurrentTimeAsString()

    appendLog_Parquet(
      strNow,
      level,
      step,
      msg
    )

    println(strNow + " " + level + ":" + msg)
  }

  def error(
             step:String,
             msg:String
           ): Unit ={
    val level = "ERROR"
    val strNow = getCurrentTimeAsString()

    appendLog_Parquet(
      strNow,
      level,
      step,
      msg
    )

    println(strNow + " " + level + ":" + msg)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  //INFO, 2021-03-04T12:01:00, Load Table1, Start loading
  //WARN, 2021-03-04T12:02:05, Load Table1, Spark Job failed, retry once
  /////try{execute_query} catch{case ex:Exception => Log("ERROR", ex.toString}
  //ERROR, 2021-03-04T12:05:11, Load Table1, File not found
  //INFO, 2021-03-04T12:01:00, Invalidate Impala Table for table1, Open Impala JDBC Connection
  //INFO, 2021-03-04T12:01:01, Invalidate Impala Table for table1, Execute Impala JDBC Query
  //INFO, 2021-03-04T12:01:02, Invalidate Impala Table for table1, Close Impala JDBC Connection

  case class Log(
                  job_name:String,
                  batch_id:Long,
                  level:String,
                  time:String,
                  step:String,
                  msg:String
                )

  def appendLog_Parquet(
                         time:String,
                         level:String,
                         step:String,
                         msg:String
                       ): Unit ={

    val spark = sparkSession

    if(spark == null){
      println("ERROR:sparkSession is null! Please set sparkSession!")
    }
    else {
      //    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

      val strParquetFileName__JobLog = ParquetUtil.generateRandomParquetFileName()

      val jobLogList = Seq(
        Log(
          jobName,
          batchId,
          level,
          time,
          step,
          msg)
      )

      println("Debug jobname:" + jobName)
      val jobLogPartition = "job_name=" + jobName + "/batch_id=" + batchId


      val strParquetFileS3FullPath__JobLog = ParquetUtil.generateParquetFileS3FullPath(
        strParquetFileName__JobLog,
        logFullPath,
        jobLogPartition
      )


      ParquetUtil.saveAsParquetToS3[Log](
        jobLogList,
        strParquetFileS3FullPath__JobLog,
        jobConfig.s3AccessKey,
        jobConfig.s3SecretKey,
        jobConfig.s3SessionToken
      )
    }
  }
}
