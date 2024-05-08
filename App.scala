package com.macquarie.rmg.openpages

object App {

  var parentStepName = ""
  var childStepName = ""


  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


  def changeStepName(
                      parentStepNewName:String = null,
                      childStepNewName:String = null
                    ): Unit ={

    if(parentStepNewName != null){
      parentStepName = parentStepNewName
    }

    if(childStepNewName != null){
      childStepName = childStepNewName
    }
  }


  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


  def main(args : Array[String]) {

    changeStepName(parentStepNewName = "Main function starts executing job")
    println(LogUtil.getCurrentTimeAsString() + " INFO:Start executing job!")

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    Transformation.jobConfig.flagTestLocally = System.getProperty("os.name").contains("Windows")

    if(Transformation.jobConfig.flagTestLocally == true){
      System.setProperty("hadoop.home.dir", "C:/Users/lwang33/hadoop-2.7.1")
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    var flagLogUtilIsReadyToUse = false


    try{
      changeStepName(parentStepNewName = "Transformation.initVariablesFromMainFunctionParameters(args)")
      Transformation.initVariablesFromMainFunctionParameters(args)

      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

      changeStepName(parentStepNewName = "Transformation.initSparkSession()")
      Transformation.initSparkSession()

      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

      changeStepName(parentStepNewName = "Transformation.initVariablesFromConfigFile()")
      Transformation.initVariablesFromConfigFile()

      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

      changeStepName(parentStepNewName = "Transformation.initLogUtil()")
      Transformation.initLogUtil()

      flagLogUtilIsReadyToUse = true

      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//      changeStepName(parentStepNewName = "Transformation.initConfigMap__Impala()")
//      Transformation.initConfigMap__Impala()

      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      changeStepName(parentStepNewName = "Transformation.checkIfExists__JobLog()")
      Transformation.checkIfExists__JobLog()

      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

      changeStepName(parentStepNewName = "Transformation.initS3Client()")
      Transformation.initS3Client()

      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

      changeStepName(parentStepNewName = "Transformation.executeIngestionSteps(changeStepName)")
      Transformation.executeTransformationSteps(changeStepName)

      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

      changeStepName(parentStepNewName = "Transformation.refreshJobLogTable()")
      Transformation.refreshJobLogTable()

    }
    catch{
      case ex:Exception =>
        val strError = "ERROR:" + ex.toString
        println(LogUtil.getCurrentTimeAsString() + " " + strError)

        if(flagLogUtilIsReadyToUse == true){
          LogUtil.error(parentStepName + ", " + childStepName, strError)
        }

        throw ex
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    println(LogUtil.getCurrentTimeAsString() + " INFO:Finish executing job!")
    println(LogUtil.getCurrentTimeAsString() + " INFO:All done!")

  }

}
