package com.macquarie.rmg.openpages

object ParamUtil extends Serializable {

  import scala.collection.mutable.Map

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


  def prepareConfigFromArg_JobConfig(
                                      args : Array[String],
                                      paramKeyList: Array[String]
                                    ): Map[String, String] ={

    val configMap = Map[String, String]()

    paramKeyList
      .foreach{paramKey =>
        try{
          val paramValue = args
            .filter{p =>
              p.contains(paramKey)
            }
            .head
            .trim
            .split("=")
            .last

          configMap += (paramKey -> paramValue)
        }
        catch{
          case ex:Exception =>
            throw new Exception("Error parsing paramKey:\"" + paramKey + "\"!\n" + "Error:" + ex.toString)
        }

      }

    configMap
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


}
