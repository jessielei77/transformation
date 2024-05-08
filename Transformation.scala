package com.macquarie.rmg.openpages

object Transformation extends Serializable {

  import org.apache.spark.storage.StorageLevel
  import java.sql.Timestamp
  import java.util.concurrent.{Callable, Executors, ThreadPoolExecutor, TimeUnit}

  import com.amazonaws.services.s3.AmazonS3
  import org.apache.spark.sql.{DataFrame, SparkSession}
  import org.joda.time.DateTime
  import org.joda.time.format.DateTimeFormat

  import scala.collection.mutable.ArrayBuffer
  import scala.collection.mutable.Map
  import scala.util.Try
  import org.apache.spark.sql.functions._


  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


  var jobConfig:JobConfig     = JobConfig()
  var spark:SparkSession      = null
  var s3Client:AmazonS3       = null
  var flagTableExists__JobLog = false
  var currentTime: String = null

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


  def initVariablesFromMainFunctionParameters(
                                               args:Array[String]
                                             ): Unit ={

    val paramKeyList = Array(
      "jobName", //openpages-etl
      "batchId", //20210502120001
      "configFileFullPath", //file:///C:/Users/lwang33/Documents/openpages/source_code_jdbc/openpages/ingestion.json
      "s3AccessKey",
      "s3SecretKey",
      "s3SessionToken"
//	  ,

//      "jassFilePath",
//      "impalaHostname",
//      "impalaPort",
//      "impalaFQDNHost",
//      "realm"
    )

    //Prepare configMap_JobConfig by reading from args of this main function
    jobConfig.configMap_JobConfig = ParamUtil.prepareConfigFromArg_JobConfig(
      args,
      paramKeyList
    )

    jobConfig.jobName                   = jobConfig.configMap_JobConfig.getOrElse("jobName", "")
    jobConfig.batchId                   = jobConfig.configMap_JobConfig.getOrElse("batchId", "").toLong
    jobConfig.configFileFullPath        = jobConfig.configMap_JobConfig.getOrElse("configFileFullPath", null)
    jobConfig.s3AccessKey               = jobConfig.configMap_JobConfig.getOrElse("s3AccessKey", null)
    jobConfig.s3SecretKey               = jobConfig.configMap_JobConfig.getOrElse("s3SecretKey", null)
    jobConfig.s3SessionToken            = jobConfig.configMap_JobConfig.getOrElse("s3SessionToken", null)

//    jobConfig.jassFilePath              = jobConfig.configMap_JobConfig.getOrElse("jassFilePath", null)
//    jobConfig.impalaHostname            = jobConfig.configMap_JobConfig.getOrElse("impalaHostname", null)
//    jobConfig.impalaPort                = jobConfig.configMap_JobConfig.getOrElse("impalaPort", null)
//    jobConfig.impalaFQDNHost            = jobConfig.configMap_JobConfig.getOrElse("impalaFQDNHost", null)
//    jobConfig.realm                     = jobConfig.configMap_JobConfig.getOrElse("realm", null)

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //Set configFullPath in ConfigUtil
    ConfigUtil.configFullPath           = jobConfig.configFileFullPath
  }


  def initSparkSession(): Unit ={

    //Initial Spark Session
    var sparkBuilder = SparkSession
      .builder()
      .enableHiveSupport
      .appName(jobConfig.jobName)

    if(jobConfig.flagTestLocally == true){
      sparkBuilder = sparkBuilder.master("local[*]")
    }

    if(jobConfig.s3AccessKey != null && jobConfig.s3SecretKey != null && jobConfig.s3SessionToken != null){
      sparkBuilder = sparkBuilder
        .config("fs.s3a.aws.credentials.provider",  "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
        .config("fs.s3a.access.key",                jobConfig.s3AccessKey)
        .config("fs.s3a.secret.key",                jobConfig.s3SecretKey)
        .config("fs.s3a.session.token",             jobConfig.s3SessionToken)
    }

    spark = sparkBuilder.getOrCreate()

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //Set Spark Session in ConfigUtil
    ConfigUtil.sparkSession = spark
    val format = new java.text.SimpleDateFormat("yyyyMMddHHmmssSSS")
    currentTime = format.format(new java.util.Date())
  }


  def initVariablesFromConfigFile(): Unit = {

    //Read JSON values from config file i.e. ingestion.json
    val strJobConfig = ConfigUtil.readFromJobConfigFile()

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    jobConfig.projectName = JSONUtil.getJSONNodeAttributeAsString(strJobConfig, "projectName").trim
    jobConfig.s3Bucket = JSONUtil.getJSONNodeAttributeAsString(strJobConfig, "s3Bucket").trim
    jobConfig.s3ProjectFolder = JSONUtil.getJSONNodeAttributeAsString(strJobConfig, "s3ProjectFolder").trim
    jobConfig.projectDB = JSONUtil.getJSONNodeAttributeAsString(strJobConfig, "projectDB").trim
    jobConfig.jobLogTableName = JSONUtil.getJSONNodeAttributeAsString(strJobConfig, "jobLogTableName").trim
    jobConfig.jobLogTableFullName = jobConfig.projectDB + "." + jobConfig.jobLogTableName
    jobConfig.jobLogTableS3FullPath = "s3a://" + jobConfig.s3Bucket + "/" + jobConfig.s3ProjectFolder + "/" + jobConfig.jobLogTableName
    jobConfig.jobType = JSONUtil.getJSONNodeAttributeAsString(strJobConfig,"jobType").trim
    jobConfig.sourceJoinSnapshotTablesList = JSONUtil.getJSONNodeAttributeAsString(strJobConfig,"sourceJoinSnapshotTablesList").trim.split(",").toList

    Try {
      jobConfig.transformationParallelism = JSONUtil.getJSONNodeAttributeAsString(strJobConfig, "transformationParallelism").trim.toInt
    }
    if (jobConfig.transformationParallelism < 1) {
      jobConfig.transformationParallelism = 1
    }
    if (jobConfig.transformationParallelism > 10) {
      jobConfig.transformationParallelism = 10
    }

    Try {
      jobConfig.transformationMaxWaitTimeInMinutes = JSONUtil.getJSONNodeAttributeAsString(strJobConfig, "transformationMaxWaitTimeInMinutes").trim.toInt
    }
    if (jobConfig.transformationMaxWaitTimeInMinutes < 1) {
      jobConfig.transformationMaxWaitTimeInMinutes = 1
    }
    if (jobConfig.transformationMaxWaitTimeInMinutes > 600) {
      jobConfig.transformationMaxWaitTimeInMinutes = 600
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //Read job step configs from stepList
    val stepNodeList = JSONUtil.getJSONNodeList(strJobConfig, "stepList")

    jobConfig.stepConfigList__Transformation = stepNodeList
      .filter { stepNode =>
        val stepType     = JSONUtil.getJSONNodeAttributeAsString(stepNode, "stepType").trim

        stepType.toLowerCase == "transformation"
      }
      .map { stepNode =>
        val stepConfig__Transformation = StepConfig__Transformation()

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        stepConfig__Transformation.stepCategory = JSONUtil.getJSONNodeAttributeAsString(stepNode, "stepCategory").trim

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        val sourceJoinTableNodeList = JSONUtil.getJSONNodeList(stepNode, "sourceJoinTableList")
        stepConfig__Transformation.sourceJoinTableList = sourceJoinTableNodeList
          .map { sourceJoinTableNode =>
            val sourcePath = JSONUtil.getJSONNodeAttributeAsString(sourceJoinTableNode, "sourcePath").trim
            val viewName = JSONUtil.getJSONNodeAttributeAsString(sourceJoinTableNode, "viewName").trim
            val cacheFlag = JSONUtil.getJSONNodeAttributeAsString(sourceJoinTableNode, "cacheFlag").trim

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////

            SourceJoinTable(sourcePath, viewName,cacheFlag)
          }

        stepConfig__Transformation.sourceJoinSQL = JSONUtil.getJSONNodeAttributeAsString(stepNode, "sourceJoinSQL").trim

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        if(stepConfig__Transformation.stepCategory.toLowerCase() == "object"){
          val lookupNodeList = JSONUtil.getJSONNodeList(stepNode, "lookupList")
          stepConfig__Transformation.lookupList = lookupNodeList
            .map { lookupNode =>
              val lookupListName = JSONUtil.getJSONNodeAttributeAsString(lookupNode, "lookupListName").trim
              val lookupListType = JSONUtil.getJSONNodeAttributeAsString(lookupNode, "lookupListType").trim
              val lookupListSourcePath = JSONUtil.getJSONNodeAttributeAsString(lookupNode, "lookupListSourcePath").trim
              val lookupListSourceSQL = JSONUtil.getJSONNodeAttributeAsString(lookupNode, "lookupListSourceSQL").trim

              ////////////////////////////////////////////////////////////////////////////////////////////////////////////

              Lookup(lookupListName, lookupListType, lookupListSourcePath,lookupListSourceSQL)
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        Try {
          stepConfig__Transformation.numOfWriteRepartition = JSONUtil.getJSONNodeAttributeAsString(stepNode, "numOfWriteRepartition").trim.toInt
        }
        if (stepConfig__Transformation.numOfWriteRepartition < 1) {
          stepConfig__Transformation.numOfWriteRepartition = 1
        }
        if (stepConfig__Transformation.numOfWriteRepartition > 40) {
          stepConfig__Transformation.numOfWriteRepartition = 40
        }

        stepConfig__Transformation.hiveTableName = JSONUtil.getJSONNodeAttributeAsString(stepNode, "hiveTableName").trim
        stepConfig__Transformation.hiveTableFullName = jobConfig.projectDB + "." + stepConfig__Transformation.hiveTableName

        stepConfig__Transformation.hiveTableS3Path = jobConfig.s3ProjectFolder + "/" + stepConfig__Transformation.hiveTableName
        stepConfig__Transformation.hiveTableS3FullPath = "s3a://" + jobConfig.s3Bucket + "/" + jobConfig.s3ProjectFolder + "/" + stepConfig__Transformation.hiveTableName
        stepConfig__Transformation.s3CheckpointPath = "s3a://" + jobConfig.s3Bucket + "/" + jobConfig.s3ProjectFolder

        val preDerivedColumnNodeList = JSONUtil.getJSONNodeList(stepNode, "preDerivedColumnList")
        stepConfig__Transformation.preDerivedColumnList = preDerivedColumnNodeList
          .map { preDerivedColumnNode =>
            val preDerivedColExpression = JSONUtil.getJSONNodeAttributeAsString(preDerivedColumnNode, "colExpression").trim
            val preDerivedcolSource = JSONUtil.getJSONNodeAttributeAsString(preDerivedColumnNode, "colSource").trim
            val preDerivedcolSourceExpression = JSONUtil.getJSONNodeAttributeAsString(preDerivedColumnNode, "colSourceExpression").trim
            val preDerivedcolAlias = JSONUtil.getJSONNodeAttributeAsString(preDerivedColumnNode, "colAlias").trim

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////

            DerivedColumn(preDerivedColExpression, preDerivedcolSource, preDerivedcolSourceExpression, preDerivedcolAlias)
          }

        val postDerivedColumnNodeList = JSONUtil.getJSONNodeList(stepNode, "postDerivedColumnList")
        stepConfig__Transformation.postDerivedColumnList = postDerivedColumnNodeList
          .map { postDerivedColumnNode =>
            val postDerivedColExpression = JSONUtil.getJSONNodeAttributeAsString(postDerivedColumnNode, "colExpression").trim


            val postDerivedcolSource = JSONUtil.getJSONNodeAttributeAsString(postDerivedColumnNode, "colSource").trim
            val postDerivedcolSourceExpression = JSONUtil.getJSONNodeAttributeAsString(postDerivedColumnNode, "colSourceExpression").trim
            val postDerivedcolAlias = JSONUtil.getJSONNodeAttributeAsString(postDerivedColumnNode, "colAlias").trim

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////

            DerivedColumn(postDerivedColExpression, postDerivedcolSource, postDerivedcolSourceExpression, postDerivedcolAlias)
          }

        val partitionColumnNodeList = JSONUtil.getJSONNodeList(stepNode, "partitionColumnList")
        stepConfig__Transformation.partitionColumnList = partitionColumnNodeList
          .map { partitionColumnNode =>
            val partitionColumn = JSONUtil.getJSONNodeAttributeAsString(partitionColumnNode, "value").trim

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////

            partitionColumn
          }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        stepConfig__Transformation
      }

  }

  def checkIfExists__JobLog(
                             spark:SparkSession = spark
                           ): Boolean ={

//    LogUtil.info("Check if job log table exists", "Start checking job log table.")

    flagTableExists__JobLog = spark.catalog.tableExists(jobConfig.jobLogTableFullName)

    //    LogUtil.info("Check if job log table exists", "Finish checking job log table.")

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    flagTableExists__JobLog
  }




//  def checkIfExists__JobLog(): Boolean ={

//    flagTableExists__JobLog = ImpalaUtil.impalaCheckIfTableExists(
//      jobConfig.configMap__Impala,
//      "Check if job log table exists in Impala",
//      jobConfig.projectDB,
//      jobConfig.jobLogTableName
//    )

    //    println("flagTableExists__JobLog value is:" + flagTableExists__JobLog)
    //    LogUtil.info("Check if job log table exists", "Finish checking job log table.")

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//    flagTableExists__JobLog

//  }


  def initLogUtil(): Unit ={

    //Initial LogUtil
    LogUtil.sparkSession      = spark
    LogUtil.projectName       = jobConfig.projectName
    LogUtil.jobName           = jobConfig.jobName
    LogUtil.batchId           = jobConfig.batchId
    LogUtil.logFullPath       = jobConfig.jobLogTableS3FullPath
    LogUtil.logTableFullName  = jobConfig.jobLogTableFullName
  }


  def initS3Client(): Unit ={

    //Initial s3Client
    LogUtil.info("Initial s3Client", "Start initialising s3Client.")

    s3Client = S3Util.initS3Client(
      jobConfig.s3AccessKey,
      jobConfig.s3SecretKey,
      jobConfig.s3SessionToken
    )

    //    LogUtil.info("Initial s3Client", "Finish initialising s3Client.")
  }


//  def initConfigMap__Impala(): Unit ={

    //Prepare configMap__Impala
    //    LogUtil.info("Prepare configMap__Impala", "Start preparing configMap__Impala.")

//    jobConfig.configMap__Impala = ImpalaUtil.prepareConfigMap(
//      jobConfig.jassFilePath,
//      jobConfig.impalaHostname,
//      jobConfig.impalaPort,
//      jobConfig.impalaFQDNHost,
//      jobConfig.realm
//    )

    //    LogUtil.info("Prepare configMap__Impala", "Finish preparing configMap__Impala.")
//  }


  def generateLookupListAndRegisterUDF(
                                        stepConfigList__Transformation: Seq[StepConfig__Transformation]
                                      ): Unit ={

    var lookupTableList = scala.collection.mutable.Map[String, Any]()

    generateSourceTables(stepConfigList__Transformation)

    LogUtil.info("Generate look up list", "start generating look up list.")

    val lookupList__Distinct = stepConfigList__Transformation
      .flatMap{stepConfig__Transformation =>
        stepConfig__Transformation.lookupList
      }
      .distinct

    lookupList__Distinct.foreach
    {
      lookup__Distinct=>
        LogUtil.info("Generate look up list", "Look up table name is :" + lookup__Distinct.lookupListName)
    }

    if(lookupList__Distinct.nonEmpty){
      lookupList__Distinct
        .foreach{lookup__Distinct =>

          val idValueList = scala.collection.mutable.Map[Long, String]()
          val userValueList = scala.collection.mutable.Map[String, String]()

          //////////////////////////////////////////////////////////////////////////////////////////////////////////////

          LogUtil.info("Generate look up list", "For look up table \"" + lookup__Distinct.lookupListName + "\", start generating look up table.")

          val dfLookup = spark.read.option("mergeSchema", "true").parquet(lookup__Distinct.lookupListSourcePath)
          dfLookup.createOrReplaceTempView(lookup__Distinct.lookupListName)

          val lookupSourceSQL = """
                                     |lookupListSourceSQL
                                     |""".stripMargin
            .replace("lookupListSourceSQL", lookup__Distinct.lookupListSourceSQL)

          LogUtil.info("Executing lookupSourceSQL","For look up table : " + lookup__Distinct.lookupListName)
          val lookupSource = spark.sql(lookupSourceSQL)
//          lookupSource.persist(StorageLevel.DISK_ONLY)

          if (lookup__Distinct.lookupListType.toLowerCase == "id") {
            lookupSource
              .collect()
              .foreach { row =>
                val lookup_id = row.getLong(0)
                val lookup_value = row(1).toString

                idValueList += lookup_id -> lookup_value
              }

            LogUtil.info("Generate look up list", "Look up table \"" + lookup__Distinct.lookupListName + "\" size: " + idValueList.size)

            //////////////////////////////////////////////////////////////////////////////////////////////////////////////

            lookupTableList += lookup__Distinct.lookupListName -> idValueList
          }

          if (lookup__Distinct.lookupListType.toLowerCase == "user"){
            lookupSource
              .collect()
              .foreach { row =>
                val lookup_user = row(0).toString
                val lookup_value = row(1).toString

                userValueList += lookup_user -> lookup_value
              }

            LogUtil.info("Generate look up list", "Look up table \"" + lookup__Distinct.lookupListName + "\" size: " + userValueList.size)

            //////////////////////////////////////////////////////////////////////////////////////////////////////////////

            lookupTableList += lookup__Distinct.lookupListName -> userValueList
          }
        }


      val lookupValue = LookupValue(
        lookupTableList = lookupTableList
      )


      val lookupValueByIdList = udf { (
                                        lookupTableName:String,
                                        lookupId: String,
                                        splitDelimiter: String,
                                        outputDelimiter: String,
                                        descriptionForNotMatch: String
                                      ) => {


        var strOutput:String = null
        val lookupResultList = new ArrayBuffer[String]()

        try {
          if (lookupId != null) {
            //          Try
            {
              val splitList0 = lookupId.split(splitDelimiter, -1)

              var splitList = splitList0

              if (splitList0.length >= 2) {
                splitList = splitList0
                  .zipWithIndex
                  .filter { case (splitElement, index) =>
                    var flagEligibleElement = false

                    if (index == 0 && splitElement.trim == "") {
                      flagEligibleElement == false
                    }
                    else if (index == splitList0.length - 1 && splitElement.trim == "") {
                      flagEligibleElement == false
                    }
                    else {
                      flagEligibleElement = true
                    }

                    flagEligibleElement
                  }
                  .map { case (splitElement, index) =>
                    splitElement
                  }
              }

              if (splitList.nonEmpty) {
                val lookupVar_id_value = lookupValue.lookupTableList(lookupTableName).asInstanceOf[scala.collection.mutable.Map[Long, String]]
                val idValueList = lookupVar_id_value

                splitList
                  .foreach { splitElement =>
                    var lookupResult = descriptionForNotMatch

                    Try {
                      val lookupId__Long = splitElement.toDouble.toLong

                      val lookupResult0 = idValueList(lookupId__Long)

                      if (lookupResult0 != null) {
                        lookupResult = lookupResult0
                      }
                    }

                    lookupResultList += lookupResult
                  }
              }
            }

            strOutput = lookupResultList.mkString(outputDelimiter)
          }
        }
        catch{
          case ex:Exception =>
            throw new Exception("Error:\"" + ex.toString)
//            strOutput = "UDF broadcastedValue.value.lookupTableList size: " + broadcastValue.lookupTableList.size + ", Error:" + ex.toString
        }

        strOutput
      }
      }

      val lookupValueByUserList = udf { (
                                        lookupTableName:String,
                                        lookupUser: String,
                                        splitDelimiter: String,
                                        outputDelimiter: String,
                                        descriptionForNotMatch: String
                                      ) => {


        var strOutput:String = null
        val lookupResultList = new ArrayBuffer[String]()

        try {
          if (lookupUser != null) {
            //          Try
            {
              val splitList0 = lookupUser.split(splitDelimiter, -1)

              var splitList = splitList0

              if (splitList0.length >= 2) {
                splitList = splitList0
                  .zipWithIndex
                  .filter { case (splitElement, index) =>
                    var flagEligibleElement = false

                    if (index == 0 && splitElement.trim == "") {
                      flagEligibleElement == false
                    }
                    else if (index == splitList0.length - 1 && splitElement.trim == "") {
                      flagEligibleElement == false
                    }
                    else {
                      flagEligibleElement = true
                    }

                    flagEligibleElement
                  }
                  .map { case (splitElement, index) =>
                    splitElement
                  }
              }

              if (splitList.nonEmpty) {
                val lookupVar_user_value = lookupValue.lookupTableList(lookupTableName).asInstanceOf[scala.collection.mutable.Map[String, String]]
                val userValueList = lookupVar_user_value

                splitList
                  .foreach { splitElement =>
                    var lookupResult = descriptionForNotMatch

                    Try {
                      val lookupId__String = splitElement.toString

                      val lookupResult0 = userValueList(lookupId__String)

                      if (lookupResult0 != null) {
                        lookupResult = lookupResult0
                      }
                    }

                    lookupResultList += lookupResult
                  }
              }
            }

            strOutput = lookupResultList.mkString(outputDelimiter)
          }

          //          strOutput = lookupResultList.mkString(outputDelimiter)
          //      if(lookupResultList.length > 1){
          //        strOutput = outputDelimiter + strOutput + outputDelimiter
          //      }
        }
        catch{
          case ex:Exception =>
            throw new Exception("Error:\"" + ex.toString)
          //            strOutput = "UDF broadcastedValue.value.lookupTableList size: " + broadcastValue.lookupTableList.size + ", Error:" + ex.toString
        }

        strOutput
      }
      }

      spark.udf.register("lookupValueByIdList", lookupValueByIdList)
      spark.udf.register("lookupValueByUserList", lookupValueByUserList)

    }
  }

  def generateSourceTables(
                          stepConfigList__Transformation: Seq[StepConfig__Transformation]
                      ): Unit ={

    //Generate dfSource
    LogUtil.info("Generate Source join tables ", "Generating source join tables and checkpointing them..")

    try{

      val s3CheckpointPath = stepConfigList__Transformation.head.s3CheckpointPath

      val sourceTableListDistinct = stepConfigList__Transformation.filter(x => x.stepCategory.equalsIgnoreCase("relationship") || x.stepCategory.equalsIgnoreCase("object") )
        .flatMap{stepConfig__Transformation => stepConfig__Transformation.sourceJoinTableList
        }.distinct

      sourceTableListDistinct.foreach { sourceJoinTable =>

        val sourceJoinTablePath = sourceJoinTable.sourcePath
        val sourceJoinTableViewName = sourceJoinTable.viewName
        val sourceJoinTableCacheFlag = sourceJoinTable.cacheFlag

        if(jobConfig.jobType.equalsIgnoreCase("daily") && jobConfig.sourceJoinSnapshotTablesList.contains(sourceJoinTableViewName)){

          val sourceTablePath = jobConfig.s3ProjectFolder.replace("integration","validated")+"/"+sourceJoinTableViewName+"/"
          LogUtil.info("S3 listing","Fetching latest snapshot from s3 path : "+ sourceTablePath)
          val latestSourceJoinTablePath: String = fetchLatestSnapshotS3Path(sourceTablePath)
          if(latestSourceJoinTablePath == null || latestSourceJoinTablePath.isEmpty) throw new Exception("Latest snapshot dir is either null or empty ")
          val latestSourceJoinTableSnapshot = latestSourceJoinTablePath.substring(latestSourceJoinTablePath.indexOf("batch_id"),latestSourceJoinTablePath.lastIndexOf("/"))
          LogUtil.info("S3 listing","Fetched latest snapshot for s3 path "+ sourceJoinTablePath+" : "+latestSourceJoinTableSnapshot)
          val sourceTablePathDF = spark.read.option("mergeSchema", "true").parquet(sourceJoinTablePath+latestSourceJoinTableSnapshot+"/")
          sourceTablePathDF.registerTempTable(sourceJoinTableViewName)
          if(sourceJoinTableCacheFlag.toLowerCase == "true"){
            persistDF(sourceTablePathDF)
            LogUtil.info("debug", "For table \"" + sourceJoinTableViewName + "\", persisted..")
          }
        }else if(jobConfig.jobType.equalsIgnoreCase("historical") ){
          val sourceTablePathDF = spark.read.option("mergeSchema", "true").parquet(sourceJoinTablePath)
          LogUtil.info("Generate Source join tables","Checkpointing source join table for view name "+ sourceJoinTableViewName)
          sourceTablePathDF.repartition(200).write.mode("append").parquet(s3CheckpointPath+"/checkpoint/"+sourceJoinTableViewName+"/"+currentTime+"/")
          val sourceTableDF = spark.read.parquet(s3CheckpointPath+"/checkpoint/"+sourceJoinTableViewName+"/"+currentTime+"/")
          LogUtil.info("Generate Source join tables","Registering source join table for view name "+ sourceJoinTableViewName)
          sourceTableDF.registerTempTable(sourceJoinTableViewName)
          if(sourceJoinTableCacheFlag.toLowerCase == "true"){
            persistDF(sourceTableDF)
            LogUtil.info("debug", "For table \"" + sourceJoinTableViewName + "\", persisted..")
          }
        }else{
          val sourceTablePathDF = spark.read.option("mergeSchema", "true").parquet(sourceJoinTablePath)
          LogUtil.info("Generate Source join tables","Reading complete data for table : "+ sourceJoinTableViewName)
          sourceTablePathDF.registerTempTable(sourceJoinTableViewName)
          if(sourceJoinTableCacheFlag.toLowerCase == "true"){
            persistDF(sourceTablePathDF)
            LogUtil.info("debug", "For table \"" + sourceJoinTableViewName + "\", persisted..")
          }
        }

      }
    }catch{
      case e:Exception => LogUtil.error("Generate source join tables",e.getMessage)
      throw new Exception("Exception inside generateSourcesTables..")
    }

  }

  def generateDFSource(
                        stepConfig__Transformation: StepConfig__Transformation
                      ): Unit ={

    //Generate dfSource
    LogUtil.info("Generate dfSource", "For table \"" + stepConfig__Transformation.hiveTableFullName + "\", start generating dfSource.")

    val sourceJoinSQL__Filtered = """
                                      |sourceJoinSQL
                                      |""".stripMargin
        .replace("sourceJoinSQL", stepConfig__Transformation.sourceJoinSQL)

      val dfSource0 = spark.sql(sourceJoinSQL__Filtered)

      dfSource0.createOrReplaceTempView("dfSource0__" + stepConfig__Transformation.hiveTableFullName.replace(".", "_"))

      LogUtil.info("Generate dfSource", "For table \"" + stepConfig__Transformation.hiveTableFullName + "\", dfSource0 generated" )

      ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

      val strCastQueryList = new ArrayBuffer[String]()
      val strCastQueryColList = new ArrayBuffer[String]()

      strCastQueryList += "SELECT "

      dfSource0.schema
        .foreach{col =>
          if(col.name.toLowerCase.endsWith("_id") && col.dataType.toString.toLowerCase.trim != "stringtype"){
            val strColumnLine = "  CAST(`" + col.name + "` AS LONG) AS " + col.name

            strCastQueryColList += strColumnLine
          }
          else {
            strCastQueryColList += col.name
          }
        }

      val strCastColList = strCastQueryColList.mkString(",\n")
      strCastQueryList += strCastColList
      strCastQueryList += "FROM "
      strCastQueryList += "  " + "dfSource0__" + stepConfig__Transformation.hiveTableFullName.replace(".", "_")

      val CastQuery = strCastQueryList.mkString("\n")
      LogUtil.info("Generate dfSource", "For table \"" + stepConfig__Transformation.hiveTableFullName + "\", CastQuery:" + CastQuery )

      val dfSource = spark.sql(CastQuery)
      dfSource.createOrReplaceTempView("dfSource__" + stepConfig__Transformation.hiveTableFullName.replace(".", "_"))

//    }

    //              LogUtil.info("Generate dfSource", "For table \"" + hiveTableFullName + "\", finish generating dfSource.")
  }


  def generateDFSource__DerivedColumns(
                                        stepConfig__Transformation: StepConfig__Transformation
                                      ): DataFrame = {

    //Generate dfSource__DerivedColumns
    LogUtil.info("Generate dfSource__DerivedColumns", "For table \"" + stepConfig__Transformation.hiveTableFullName + "\", start generating dfSource__DerivedColumns.")

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    var strSQL__ColumnList = "*"

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    if(stepConfig__Transformation.preDerivedColumnList.nonEmpty){

      val sqlPreDerivedColumns = stepConfig__Transformation.preDerivedColumnList
        .zipWithIndex
        .map{case (derivedColumn, index) =>
          var comma = ","
          if(index == stepConfig__Transformation.preDerivedColumnList.length - 1){
            comma = ""
          }

          //////////////////////////////////////////////////////////////////////////////////////////////////////////////

          var derivedColumnSQLLine = derivedColumn.colExpression + " AS " + derivedColumn.colAlias

          //////////////////////////////////////////////////////////////////////////////////////////////////////////////

          if(derivedColumn.colSource.toLowerCase == "param"){
            val colSourceExpressionValue = jobConfig.configMap_JobConfig.getOrElse(derivedColumn.colSourceExpression, null)

            if(colSourceExpressionValue == null){
              throw new Exception("Error:\"" + derivedColumn.colSourceExpression + "\" not found from parameters!")
            }
            else {
              //e.g.CAST(batchId AS LONG) replaced "batchId" with the batchId value from params
              derivedColumnSQLLine = derivedColumn.colExpression.replace(derivedColumn.colSourceExpression, colSourceExpressionValue) + " AS " + derivedColumn.colAlias
            }
          }

          //////////////////////////////////////////////////////////////////////////////////////////////////////////////

          derivedColumnSQLLine = derivedColumnSQLLine + comma

          //////////////////////////////////////////////////////////////////////////////////////////////////////////////

          derivedColumnSQLLine
        }
        .mkString("\n")

      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

      strSQL__ColumnList = sqlPreDerivedColumns + ",\n" + strSQL__ColumnList

      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

      LogUtil.info("Generate dfSource__DerivedColumns", "For table \"" + stepConfig__Transformation.hiveTableFullName + "\", preDerivedColumnList is not empty.")
    }
    else {
      LogUtil.info("Generate dfSource__DerivedColumns", "For table \"" + stepConfig__Transformation.hiveTableFullName + "\", preDerivedColumnList is empty.")
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    if(stepConfig__Transformation.postDerivedColumnList.nonEmpty){

      val sqlPostDerivedColumns = stepConfig__Transformation.postDerivedColumnList
        .zipWithIndex
        .map{case (derivedColumn, index) =>
          var comma = ","
          if(index == stepConfig__Transformation.postDerivedColumnList.length - 1){
            comma = ""
          }

          //////////////////////////////////////////////////////////////////////////////////////////////////////////////

          var derivedColumnSQLLine = derivedColumn.colExpression + " AS " + derivedColumn.colAlias

          //////////////////////////////////////////////////////////////////////////////////////////////////////////////

          if(derivedColumn.colSource.toLowerCase == "param"){
            val colSourceExpressionValue = jobConfig.configMap_JobConfig.getOrElse(derivedColumn.colSourceExpression, null)

            if(colSourceExpressionValue == null){
              throw new Exception("Error:\"" + derivedColumn.colSourceExpression + "\" not found from parameters!")
            }
            else {
              //e.g.CAST(batchId AS LONG) replaced "batchId" with the batchId value from params
              derivedColumnSQLLine = derivedColumn.colExpression.replace(derivedColumn.colSourceExpression, colSourceExpressionValue) + " AS " + derivedColumn.colAlias
            }
          }

          //////////////////////////////////////////////////////////////////////////////////////////////////////////////

          derivedColumnSQLLine = derivedColumnSQLLine + comma

          //////////////////////////////////////////////////////////////////////////////////////////////////////////////

          derivedColumnSQLLine
        }
        .mkString("\n")

      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

      strSQL__ColumnList = strSQL__ColumnList + ",\n" + sqlPostDerivedColumns

      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

      LogUtil.info("Generate dfSource__DerivedColumns", "For table \"" + stepConfig__Transformation.hiveTableFullName + "\", postDerivedColumnList is not empty.")
    }
    else {
      LogUtil.info("Generate dfSource__DerivedColumns", "For table \"" + stepConfig__Transformation.hiveTableFullName + "\", postDerivedColumnList is empty.")
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    val dfSource__DerivedColumns = spark.sql(
      """
        |SELECT
        | strSQL__ColumnList
        |FROM
        | dfSource
        |""".stripMargin
        .replace("dfSource", "dfSource__" + stepConfig__Transformation.hiveTableFullName.replace(".", "_"))
        .replace("strSQL__ColumnList", strSQL__ColumnList)
    )

    LogUtil.info("Generate dfSource__DerivedColumns", "For table \"" + stepConfig__Transformation.hiveTableFullName + "\", dfSource__DerivedColumns generated.")

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    dfSource__DerivedColumns.createOrReplaceTempView("dfSource__DerivedColumns__" + stepConfig__Transformation.hiveTableFullName.replace(".", "_"))

    //              LogUtil.info("Generate dfSource__DerivedColumns", "For table \"" + hiveTableFullName + "\", finish generating dfSource__DerivedColumns.")

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //dfSource__DerivedColumns.show(100, false)
    //dfSource__DerivedColumns.printSchema()

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    dfSource__DerivedColumns
  }


  def checkIfExists__HiveTable(
                                stepConfig__Transformation: StepConfig__Transformation//,
//                                dfSource__DerivedColumns:DataFrame
                   ): Unit = {

    //Compare schema between dfParquet__Existing and dfSource__DerivedColumns
    LogUtil.info("Check table exists", "For table \"" + stepConfig__Transformation.hiveTableFullName + "\", start checking table exists.")

    //    var flagSchemaChanged = false

    val flagTableExists__Transformation = spark.catalog.tableExists(stepConfig__Transformation.hiveTableFullName)
    if(flagTableExists__Transformation == false){
      stepConfig__Transformation.flagSchemaChanged = true

      LogUtil.info("Check table exists", "For table \"" + stepConfig__Transformation.hiveTableFullName + "\", table doesn't exist in hive.")
    }

    LogUtil.info("Check table exists", "For table \"" + stepConfig__Transformation.hiveTableFullName + "\", flagSchemaChanged is " + stepConfig__Transformation.flagSchemaChanged.toString + ".")
    //              LogUtil.info("Compare schema between dfParquet__Existing and dfSource__DerivedColumns", "For table \"" + hiveTableFullName + "\", finish comparing schema between dfParquet__Existing and dfSource__DerivedColumns.")
  }

  //It should look for yesterday's batch timestamp only
  def deleteExistingParquetFilesForPeviousFailedBatch(
                                                       stepConfig__Transformation: StepConfig__Transformation,
                                                       dfSource__DerivedColumns:DataFrame
                                                     ): Unit = {

    //      //Delete any existing Parquet files that belong to the previous failed batchId
    LogUtil.info("Delete any existing Parquet files that belong to the previous failed business effective timestamp batch", "For table \"" + stepConfig__Transformation.hiveTableFullName + "\", start reading business effective timestamp from config.")
    //      LogUtil.info("Read lstPid from batch log table", "For table \"" + stepConfig__Ingestion.hiveTableFullName + "\", start reading maxBatchId and lstPid from batch log table.")
    //

    val viewBusinessEffeTs = "vw_business_effe_ts_" + stepConfig__Transformation.hiveTableName
    dfSource__DerivedColumns.persist(StorageLevel.DISK_ONLY)
    dfSource__DerivedColumns.createOrReplaceTempView(viewBusinessEffeTs)

    val arrBusinessEffeDt = spark.sql(
      """
        |SELECT
        | Distinct
        | business_effective_timestamp
        |FROM
        | businessEffeTsView
        |""".stripMargin
        .replace("businessEffeTsView", viewBusinessEffeTs)
    ).collect()

    if (arrBusinessEffeDt.nonEmpty) {

      val s3ParquetFileList__Existing = S3Util.listObjects(
        s3Client,
        jobConfig.s3Bucket,
        stepConfig__Transformation.hiveTableS3Path
      )

      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

      arrBusinessEffeDt
        .foreach { row =>
          val business_effective_timestamp = row.getLong(0) //20210927120000

          LogUtil.info("Delete any existing Parquet files that belong to the previous failed business effective timestamp batch", "For table \"" + stepConfig__Transformation.hiveTableFullName + "\", start deleting existing Parquet files which business effective timestamp is" + business_effective_timestamp +".")

          //////////////////////////////////////////////////////////////////////////////////////////////////////////////

          s3ParquetFileList__Existing
            .filter { case (s3Bucket0, s3Path0) =>
              s3Path0.contains("business_effective_timestamp=" + business_effective_timestamp)
            }
            .foreach{ case (s3Bucket0, s3Path0) =>
              try{
                S3Util.deleteObject(
                  s3Client,
                  s3Bucket0,
                  s3Path0
                )
              }
              catch{
                case ex0:Exception =>
                  try{
                    S3Util.deleteObject(
                      s3Client,
                      s3Bucket0,
                      s3Path0
                    )
                  }
                  catch{
                    case ex1:Exception =>
                      throw new Exception("Delete S3 Object error:" + ex1.toString)
                  }
              }
            }

          //////////////////////////////////////////////////////////////////////////////////////////////////////////////

          LogUtil.info("Delete any existing Parquet files that belong to the previous failed batchId", "For table \"" + stepConfig__Transformation.hiveTableFullName + "\", finish deleting existing Parquet files which business effective timestamp is" + business_effective_timestamp +".")

        }

    }
  }



  def writeDFSource__DerivedColumnsToS3(
                                         stepConfig__Transformation: StepConfig__Transformation,
                                         dfSource__DerivedColumns:DataFrame
                                       ): Unit ={

    //Write dfSource__DerivedColumns to S3
    LogUtil.info("Write dfSource__DerivedColumns to S3", "For table \"" + stepConfig__Transformation.hiveTableFullName + "\", start writing dfSource__DerivedColumns to S3.")

    if(stepConfig__Transformation.partitionColumnList.nonEmpty){
      LogUtil.info("Write dfSource__DerivedColumns to S3", "For table \"" + stepConfig__Transformation.hiveTableFullName + "\", partitionColumnList is not empty.")

      dfSource__DerivedColumns
        .repartition(stepConfig__Transformation.numOfWriteRepartition)
        .write
        .mode("append") //?????Overwrite or Append for Full load?????
        .partitionBy(stepConfig__Transformation.partitionColumnList:_*)
        .save(stepConfig__Transformation.hiveTableS3FullPath)
    }
    else {
      LogUtil.info("Write dfSource__DerivedColumns to S3", "For table \"" + stepConfig__Transformation.hiveTableFullName + "\", partitionColumnList is empty.")

      dfSource__DerivedColumns
        .repartition(stepConfig__Transformation.numOfWriteRepartition)
        .write
        .mode("append") //?????Overwrite or Append for Full load?????
        .save(stepConfig__Transformation.hiveTableS3FullPath)
    }

    //              LogUtil.info("Write dfSource__DerivedColumns to S3", "For table \"" + hiveTableFullName + "\", finish writing dfSource__DerivedColumns to S3.")
  }


  def recreateHiveExternalTable(
                                 stepConfig__Transformation: StepConfig__Transformation,
                                 dfSource__DerivedColumns:DataFrame
                               ): Unit ={

    var hiveQueryList = Seq[String]()

    if(stepConfig__Transformation.partitionColumnList.nonEmpty){
      hiveQueryList = Array(
        "MSCK REPAIR TABLE " + stepConfig__Transformation.hiveTableFullName
      ).toSeq
    }

    if(stepConfig__Transformation.flagSchemaChanged == true){

      //Re-create Hive external table
      LogUtil.info("Re-create Hive external table", "For table \"" + stepConfig__Transformation.hiveTableFullName + "\", start re-creating Hive external table.")

      hiveQueryList = HiveUtil.generateHiveCreateExternalTableQuery(
        dfSource__DerivedColumns,
        stepConfig__Transformation.partitionColumnList,
        stepConfig__Transformation.hiveTableFullName,
        stepConfig__Transformation.hiveTableS3FullPath
      )
    }

    hiveQueryList
      .foreach{hiveQuery =>
        println(hiveQuery)
      }

    hiveQueryList
      .foreach{hiveQuery =>
        spark.sql(hiveQuery)
      }

    //              LogUtil.info("Re-create Hive external table", "For table \"" + hiveTableFullName + "\", finish re-creating Hive external table.")

  }


//  def invalidateAndRefreshImpalaTable(
//                                       stepConfig__Transformation: StepConfig__Transformation,
//                                       dfSource__DerivedColumns:DataFrame
//                                     ): Unit ={

    //Invalidate and refresh Impala table
//    LogUtil.info("Invalidate and refresh Impala table", "For table \"" + stepConfig__Transformation.hiveTableFullName + "\", start invalidating and refreshing Impala table.")

//    if(stepConfig__Transformation.flagSchemaChanged == true){
//      ImpalaUtil.invalidateTable(
//        jobConfig.configMap__Impala,
//        "Invalidate Impala metadata",
//        jobConfig.projectDB,
//        stepConfig__Transformation.hiveTableName
//      )

//      LogUtil.info("Invalidate and refresh Impala table", "For table \"" + stepConfig__Transformation.hiveTableFullName + "\", Impala table invalidated.")
//    }

//    ImpalaUtil.refreshTable(
//      jobConfig.configMap__Impala,
//      "Invalidate and refresh Impala table",
//      jobConfig.projectDB,
//      stepConfig__Transformation.hiveTableName
//    )

    //              LogUtil.info("Invalidate and refresh Impala table", "For table \"" + hiveTableFullName + "\", finish invalidating and refreshing Impala table.")
//  }



  def refreshJobLogTable(
                          spark:SparkSession = spark
                        ): Unit = {

    if (flagTableExists__JobLog == false) {

      //hive create table statement
      val hiveQueryList__JobLog = HiveUtil.generateHiveJobLogTableQuery(
        jobConfig.jobLogTableFullName,
        jobConfig.jobLogTableS3FullPath
      )

      hiveQueryList__JobLog
        .foreach{hiveQuery =>
          println(hiveQuery)
        }

      hiveQueryList__JobLog
        .foreach{hiveQuery =>
          spark.sql(hiveQuery)
        }
      //Invalidate metadata job_log table
//      LogUtil.info("Invalidate Impala job_log table", "Start invalidating Impala job_log table.")

//      ImpalaUtil.invalidateTable(
//        jobConfig.configMap__Impala,
//        "Invalidate Impala metadata",
//        jobConfig.projectDB,
//        jobConfig.jobLogTableName
//      )

//      LogUtil.info("Invalidate Impala job_log table", "Finish invalidating Impala job_log table.")
    }
    else {
      //Hive MSCK REPAIR TABLE
      val hiveRefreshQuery = "MSCK REPAIR TABLE " + jobConfig.jobLogTableFullName
      spark.sql(hiveRefreshQuery)

      //Refresh job_log table
//      LogUtil.info("Refresh Impala job_log table", "Start refreshing Impala job_log table.")

//      ImpalaUtil.refreshTable(
//        jobConfig.configMap__Impala,
//        "Refresh Impala job_log table",
//        jobConfig.projectDB,
//        jobConfig.jobLogTableName
//      )

//      LogUtil.info("Refresh Impala job_log table", "Finish refreshing Impala job_log table.")
    }
  }


  def executorTaskScheduler(
                             taskList:Seq[((Exception) => Unit) => Unit],
                             parallelism:Int = 1,
                             maxWaitTimeInMinutes:Int = 60
                           ): Unit ={

    var processedCount = 0
    val taskResultList = new ArrayBuffer[java.util.concurrent.Future[Int]]()
    val executors = Executors.newFixedThreadPool(parallelism)

    var flagCaughtException = false

    def catchException(
                        ex:Exception
                      ): Unit ={

      flagCaughtException = true

      //      val strError = "ERROR:" + ex.toString
      //      println(LogUtil.getCurrentTimeAsString() + " " + strError)
      //      LogUtil.error("Execute tasks in parallel", strError)

      throw ex
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    taskList
      .foreach{task =>

        //Check and ensure parallelism of multiple tasks
        var threadCount = executors.asInstanceOf[ThreadPoolExecutor].getActiveCount
        while(
          threadCount == jobConfig.transformationParallelism &&
            flagCaughtException == false
        ){
          Thread.sleep(1000)
          threadCount = executors.asInstanceOf[ThreadPoolExecutor].getActiveCount
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        if(flagCaughtException == false){
          val taskCallable = new Callable[Int] {
            override def call(): Int = {
              task(
                catchException
              )

              //////////////////////////////////////////////////////////////////////////////////////////////////////////

              //Return 1 for parallel task result
              1
            }
          }

          //////////////////////////////////////////////////////////////////////////////////////////////////////////////

          val taskResult = executors.submit(taskCallable)
          taskResultList += taskResult
        }
      }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    taskResultList
      .foreach{result =>
        processedCount += result.get()
      }

    executors.shutdown()
    executors.awaitTermination(maxWaitTimeInMinutes, TimeUnit.MINUTES)
  }


  def executeTransformationSteps(
                                  changeStepName:(String, String) => Unit
                                ): Unit ={

    val parentStepName = "Transformation.executeIngestionSteps()"

    if(jobConfig.stepConfigList__Transformation.nonEmpty){

      // Generating and registering source join and look up tables in sparkSession
      generateLookupListAndRegisterUDF(jobConfig.stepConfigList__Transformation)

      val taskList = jobConfig.stepConfigList__Transformation
        .map { stepConfig__Transformation =>
          val task = (catchException:(Exception) => Unit) => {

            LogUtil.info("Transformation step","Creating task for : "+stepConfig__Transformation.hiveTableFullName+" table")
            val hiveTableFullName = stepConfig__Transformation.hiveTableFullName

            try{

              //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
              //Method to execute the sourceJoinSQL
              changeStepName(parentStepName, "generateDFSource")
              generateDFSource(
                stepConfig__Transformation
              )

              //////////////////////////////////////////////////////////////////////////////////////////////////////////

              changeStepName(parentStepName, "generateDFSource__DerivedColumns")
              val dfSource__DerivedColumns = generateDFSource__DerivedColumns(
                stepConfig__Transformation
              )

              //////////////////////////////////////////////////////////////////////////////////////////////////////////

              changeStepName(parentStepName, "checkIfExists__HiveTable")
              checkIfExists__HiveTable(
                stepConfig__Transformation
              )

              //////////////////////////////////////////////////////////////////////////////////////////////////////////
              changeStepName(parentStepName, "deleteExistingParquetFilesForPeviousFailedBatch")
              deleteExistingParquetFilesForPeviousFailedBatch(
                stepConfig__Transformation,
                dfSource__DerivedColumns
              )

              //////////////////////////////////////////////////////////////////////////////////////////////////////////
              //Writing in append mode to S3
              changeStepName(parentStepName, "writeDFSource__DerivedColumnsToS3")
              writeDFSource__DerivedColumnsToS3(
                stepConfig__Transformation,
                dfSource__DerivedColumns
              )

              //////////////////////////////////////////////////////////////////////////////////////////////////////////

              changeStepName(parentStepName, "recreateHiveExternalTable")
              recreateHiveExternalTable(
                stepConfig__Transformation,
                dfSource__DerivedColumns
              )

              //////////////////////////////////////////////////////////////////////////////////////////////////////////

//              changeStepName(parentStepName, "invalidateAndRefreshImpalaTable")
//              invalidateAndRefreshImpalaTable(
//                stepConfig__Transformation,
//                dfSource__DerivedColumns
//              )

              //Unpersisting the final DF
              unPersistDF(dfSource__DerivedColumns)

              //////////////////////////////////////////////////////////////////////////////////////////////////////////

              //              changeStepName(parentStepName, "refreshJobLogTable")
              //              refreshJobLogTable(
              //                stepConfig__Ingestion
              //              )

              //////////////////////////////////////////////////////////////////////////////////////////////////////////

              //TODO
              //try{} catch{delete newly inserted parquet files; insert aud_log_batch failed?????}

              //TODO
              //?????Check if full load works with this approach
            }
            catch{
              case ex:Exception =>
                changeStepName(parentStepName, "catchException, hiveTableFullName:\"" + hiveTableFullName + "\"")
                catchException(ex)
            }
          }

          //////////////////////////////////////////////////////////////////////////////////////////////////////////////

          task
        }

      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

      executorTaskScheduler(
        taskList,
        jobConfig.transformationParallelism,
        jobConfig.transformationMaxWaitTimeInMinutes
      )
    }
    else {
      LogUtil.warn("Read stepList from config file", "stepList in config file is empty.")
    }
  }

  def unPersistDF(dataframe : DataFrame) : Unit ={
    dataframe.unpersist
  }

  def persistDF(dataframe: DataFrame) : Unit ={
    dataframe.persist(StorageLevel.DISK_ONLY)
  }

  def fetchLatestSnapshotS3Path(s3Path: String): String={

    val s3ParquetFileList__Existing = S3Util.listObjects(
      s3Client,
      jobConfig.s3Bucket,
      s3Path
    )
    s3ParquetFileList__Existing.toList.last._2
  }
}
