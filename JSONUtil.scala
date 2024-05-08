package com.macquarie.rmg.openpages

object JSONUtil extends Serializable {
  import com.alibaba.fastjson.{JSONArray, JSONObject, JSONPath}

  import scala.collection.JavaConverters._
  import scala.collection.mutable.ArrayBuffer


  def getJSONNodeList(
                       node:Object,
                       path:String,
                       strNullValue:String = "null_string"
                     ): Seq[JSONObject] ={

    val jsonObjectList = new ArrayBuffer[JSONObject]()

    try{
      val result = JSONPath.eval(node, path)

      if(result != null){
        if(result.isInstanceOf[JSONArray]){
          result.asInstanceOf[JSONArray].asScala
            .foreach{x =>
              if(x.isInstanceOf[JSONObject]){
                jsonObjectList += x.asInstanceOf[JSONObject]
              }
              else{
                jsonObjectList += new JSONObject(Map("value" -> x).asJava)
              }
            }
        }
//        else if(result.isInstanceOf[util.ArrayList[Object]]){
//          result.asInstanceOf[util.ArrayList[Object]].asScala
//            .foreach{x =>
//              jsonObjectList += x.asInstanceOf[JSONObject]
//            }
//        }
        else if(result.toString.trim == ""){
          //Do nothing
        }
        else {
          jsonObjectList += result.asInstanceOf[JSONObject]
        }
      }
    }
    catch {
      case ex:Exception =>
        val strEx = ex.toString

        val strNode = if(node == null) {strNullValue} else {node.toString}
        val strPath = if(path == null) {strNullValue} else {path}
        val strError = "node:" + strNode + ", " + "path:" + strPath + "\n" + strEx

        throw new Exception(strError)
    }

    jsonObjectList
  }


  def getJSONNodeAttributeAsString(
                                    node:Object,
                                    path:String,
                                    strNullValue:String = "null_string"
                                  ): String ={

    var strOutput = strNullValue

    try{
      if(node != null && path != null){
        val attribute = JSONPath.eval(node, path)

        if(attribute != null){
          strOutput = attribute.toString
        }
      }
    }
    catch {
      case ex:Exception =>
        val strError = ex.toString

        strOutput = "Error:" + strError
    }

    strOutput
  }


}
