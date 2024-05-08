package com.macquarie.rmg.openpages

object HiveUtil extends Serializable  {

  import org.apache.spark.sql.DataFrame

  import scala.collection.mutable.ArrayBuffer

  def columnTypeConversionFromSparkToHive(
                                           sparkColumnType:String
                                         ): String ={
    var hiveColumnType = sparkColumnType

    if(sparkColumnType.toLowerCase.contains("long")){
      hiveColumnType = "bigint"
    }
    hiveColumnType
  }

  def partitionColumnTypeConversionFromSparkToHive(
                                                    sparkColumnType:String
                                                  ): String ={
    var hiveColumnType = sparkColumnType

    if(sparkColumnType.toLowerCase.contains("timestamp")){
      hiveColumnType = "string"
    }
    if(sparkColumnType.toLowerCase.contains("long")){
      hiveColumnType = "bigint"
    }
    hiveColumnType
  }


  def generateHiveCreateExternalTableQuery(
                                            df:DataFrame,
                                            partitionColumnList:Seq[String],
                                            hiveFullTableName:String, //e.g. "rmg_ntmr_validated_dev.test_case3",
                                            tableLocation:String //e.g. "/dev/data/validated/rmg_ntmr/hive/test2"
                                          ): Seq[String] ={

    val strQueryLineList = new ArrayBuffer[String]()
    val strCreateQueryList = new ArrayBuffer[String]()
    val strCreateQueryLineList = new ArrayBuffer[String]()

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    strQueryLineList += "DROP TABLE IF EXISTS " + hiveFullTableName

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    strCreateQueryList += "CREATE EXTERNAL TABLE IF NOT EXISTS " + hiveFullTableName + "("

    df.schema
      .foreach{col =>
        if(!partitionColumnList.contains(col.name)){
          val strColumnLine = "`" + col.name + "` " + columnTypeConversionFromSparkToHive(col.dataType.toString.replace("Type",""))
          //          val strColumnLine = "`" + col.name + "` " + col.dataType.toString.replace("Type","") + strComma

          strCreateQueryLineList += strColumnLine
        }
      }

    val strCreateQueryLine = strCreateQueryLineList.mkString(",\n")
    strCreateQueryList += strCreateQueryLine

    strCreateQueryList += ")"

    if(partitionColumnList.nonEmpty){
      strCreateQueryList += "PARTITIONED BY ("

      partitionColumnList
        .zipWithIndex
        .foreach{case (partitionColumn, index) =>
          val columnList__FilteredFromDF = df.schema.filter{column0 => column0.name == partitionColumn}

          if(columnList__FilteredFromDF.nonEmpty){
            var strComma = ","
            if(index == partitionColumnList.length - 1){
              strComma = ""
            }

            val strColumnLine = "`" + columnList__FilteredFromDF.head.name + "` " + partitionColumnTypeConversionFromSparkToHive(columnList__FilteredFromDF.head.dataType.toString.replace("Type","") ) + strComma
            //            val strColumnLine = "`" + columnList__FilteredFromDF.head.name + "` " + columnList__FilteredFromDF.head.dataType.toString.replace("Type","") + strComma

            strCreateQueryList += strColumnLine
          }
        }

      strCreateQueryList += ")"
    }

    strCreateQueryList += "STORED AS PARQUET"
    strCreateQueryList += "LOCATION '" + tableLocation + "'"

    val createQuery = strCreateQueryList.mkString("\n")

    strQueryLineList += createQuery

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    if(partitionColumnList.nonEmpty){
      strQueryLineList += "MSCK REPAIR TABLE " + hiveFullTableName
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    strQueryLineList
  }


  def generateRefreshHivePartitionQuery(
                                         hiveFullTableName:String
                                       ): String ={

    val hiveQuery = "MSCK REPAIR TABLE " + hiveFullTableName + ";"

    hiveQuery
  }


//  def generateInvalidateAndRefreshImpalaQuery(
//                                               flagSchemaChanged:Boolean,
//                                               impalaFullTableName:String
//                                             ): String ={
//
//    val strQueryLineList = new ArrayBuffer[String]()
//
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//    if(flagSchemaChanged == true){
//      strQueryLineList += "INVALIDATE METADATA " + impalaFullTableName + ";"
//    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//    strQueryLineList += "REFRESH " + impalaFullTableName + ";"

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//    val impalaQuery = strQueryLineList.mkString("\n");

//    impalaQuery
//  }



  def generateHiveJobLogTableQuery(
                                         hiveFullTableName:String, //e.g. "rmg_openpages_validated_dev.aud_log_batch",
                                         tableLocation:String //e.g. "/dev/data/validated/rmg_openpages/hive/aud_log_batch"
                                       ): Seq[String] ={

    val strQueryLineList = new ArrayBuffer[String]()
    val strCreateQueryList = new ArrayBuffer[String]()

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    strQueryLineList += "DROP TABLE IF EXISTS " + hiveFullTableName

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    strCreateQueryList += "CREATE EXTERNAL TABLE IF NOT EXISTS " + hiveFullTableName + "(" +
      "`level` string," +
      "`time` string," +
      "`step` string," +
      "`msg` string) " +
      "PARTITIONED BY (job_name string,batch_id bigint) " +
      "STORED AS PARQUET " +
      "LOCATION '" + tableLocation + "'"


    val createQuery = strCreateQueryList.mkString("\n")

    strQueryLineList += createQuery

    strQueryLineList += "MSCK REPAIR TABLE " + hiveFullTableName

    strQueryLineList
  }
}
