package com.macquarie.rmg.openpages

object ParquetUtil extends Serializable {

  import org.apache.avro.reflect.ReflectData
  import org.apache.hadoop.fs.Path
  import org.apache.parquet.avro.AvroParquetWriter
  import org.apache.parquet.hadoop.metadata.CompressionCodecName
  import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}
  import org.joda.time.DateTime

  import scala.reflect._
  import scala.reflect.runtime.universe._
  import scala.util.Random



  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


  var printInfo:Boolean = true


  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


  def generateRandomParquetFileName(): String ={

    val strParquetFileName = DateTime.now().toString("yyyyMMddHHmmssSSS") + "-" + Random.alphanumeric.filter{x => x.isLetterOrDigit}.take(5).mkString("") + ".snappy.parquet"

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    strParquetFileName
  }


  def generateParquetFileS3FullPath(
                                     parquetFileName:String,
                                     s3Folder:String,
                                     partition:String = null
                                   ):String = {

    var strParquetFileS3FullPath = s3Folder
    if(!strParquetFileS3FullPath.endsWith("/")){
      strParquetFileS3FullPath = strParquetFileS3FullPath + "/"
    }

    if(partition != null){
      strParquetFileS3FullPath = strParquetFileS3FullPath + partition
    }

    if(!strParquetFileS3FullPath.endsWith("/")){
      strParquetFileS3FullPath = strParquetFileS3FullPath + "/"
    }

    strParquetFileS3FullPath = strParquetFileS3FullPath + parquetFileName

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    strParquetFileS3FullPath
  }


  def getColumnListFromClass[IN](
                                )(implicit tag__IN: TypeTag[IN]): Seq[(String, String, Boolean)] ={

    val columnListFromClass = tag__IN.tpe
      .members
      .filter{member =>
        !member.isMethod
      }
      .map{member =>
        val fieldName = member.name.toString.trim
        val fieldType = member.typeSignature.toString.trim.toLowerCase

        (fieldName, fieldType, false)
      }
      .toSeq

    columnListFromClass
  }


  def saveAsParquetToS3[IN](
                             recordList:Seq[IN],
                             s3FullPath:String,
                             s3AccessKey:String = null,
                             s3SecretKey:String = null,
                             s3SessionToken:String = null
                           )(implicit classTag__IN: ClassTag[IN]): Unit ={

    var writer:ParquetWriter[IN] = null

    try {
      if (printInfo == true) {
        recordList
          .foreach { record =>
            println("record:" + record.toString)
          }
      }

      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

      val hadoopPath = new Path(s3FullPath)
      val conf = new org.apache.hadoop.conf.Configuration()
      conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

      if(s3AccessKey != null && s3SecretKey != null){

        conf.set("fs.s3a.access.key", s3AccessKey)
        conf.set("fs.s3a.secret.key", s3SecretKey)

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        if(s3SessionToken != null){
          conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
          conf.set("fs.s3a.session.token", s3SessionToken)
        }
      }

      //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

      val schema = ReflectData.AllowNull.get.getSchema(classTag__IN.runtimeClass)

      writer = AvroParquetWriter
        .builder[IN](hadoopPath)
        .withSchema(schema)
        .withDataModel(ReflectData.get)
        .withConf(conf)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .build()

      recordList
        .foreach { record =>
          writer.write(record)
        }
    }
    catch{
      case ex:Exception =>
        val strError = "Error saveAsParquet:" + ex.toString

        println(strError)
        throw new Exception(strError)
    }
    finally {
      if(writer != null){
        writer.close()
      }
    }
  }




}
