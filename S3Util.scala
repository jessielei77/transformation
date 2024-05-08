package com.macquarie.rmg.openpages

object S3Util extends Serializable {

  import java.io._
  import java.nio.charset.StandardCharsets
  import java.util.Date

  import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials, BasicSessionCredentials}
  import com.amazonaws.retry.PredefinedRetryPolicies
  import com.amazonaws.services.s3.model._
  import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
  import com.amazonaws.{ClientConfiguration, Protocol}
  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.fs.FileSystem

  import scala.collection.JavaConverters._
  import scala.collection.mutable.ArrayBuffer


  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  //Initiate S3 Client
  def initS3Client(
                    s3AccessKey:String = null,
                    s3SecrectKey:String = null,
                    s3SessionToken:String = null,
                    proxyURL:String = null,
                    proxyPort:String = null,
                    s3Region:String = "ap-southeast-2"
                  ): AmazonS3 ={

    val clientConfig = new ClientConfiguration
    var flagUseProxy = false

    if(proxyURL != null && proxyPort != null){
      flagUseProxy = true

      clientConfig.setProtocol(Protocol.HTTPS)
      clientConfig.setProxyHost(proxyURL)
      clientConfig.setProxyPort(proxyPort.toInt)
      clientConfig.setRetryPolicy(PredefinedRetryPolicies.getDefaultRetryPolicyWithCustomMaxRetries(1))
      clientConfig.setConnectionMaxIdleMillis(10000)
    }


    var s3Client = AmazonS3ClientBuilder
      .standard()
      .withRegion(s3Region)
      .build

    if(flagUseProxy == true){
      s3Client = AmazonS3ClientBuilder
        .standard()
        .withClientConfiguration(clientConfig)
        .withRegion(s3Region)
        .build
    }

    if(s3AccessKey != null && s3SecrectKey != null){
      if(s3SessionToken == null) {
        val awsCreds = new BasicAWSCredentials(s3AccessKey, s3SecrectKey)

        if(flagUseProxy == false){
          s3Client = AmazonS3ClientBuilder
            .standard()
            .withRegion(s3Region)
            .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
            .build
        }
        else {
          s3Client = AmazonS3ClientBuilder
            .standard()
            .withClientConfiguration(clientConfig)
            .withRegion(s3Region)
            .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
            .build
        }

      }
      else{
        val awsCreds = new BasicSessionCredentials(s3AccessKey, s3SecrectKey, s3SessionToken)

        if(flagUseProxy == false){
          s3Client = AmazonS3ClientBuilder
            .standard()
            .withRegion(s3Region)
            .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
            .build

        }
        else {
          s3Client = AmazonS3ClientBuilder
            .standard()
            .withClientConfiguration(clientConfig)
            .withRegion(s3Region)
            .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
            .build
        }

      }
    }

    s3Client
  }

  //Get S3 file list
  def listObjects(
                   s3Client: AmazonS3 = AmazonS3ClientBuilder.standard().withRegion("ap-southeast-2").build,
                   s3BucketName: String,
                   s3Path: String
                 ): ArrayBuffer[(String, String)] = {
    var s3PathWrapped = s3Path
    if (!s3PathWrapped.endsWith("/")) {
      s3PathWrapped = s3Path + "/"
    }

    LogUtil.info("s3 object Listing","Listing s3 objects for path : "+s3Path)
    var s3Listing = s3Client.listObjects(s3BucketName, s3PathWrapped)
    val s3Objects = s3Listing.getObjectSummaries

    while (s3Listing.isTruncated) {
      s3Listing = s3Client.listNextBatchOfObjects(s3Listing)
      s3Objects.addAll(s3Listing.getObjectSummaries)
    }

    val objectList = new ArrayBuffer[(String, String)]()
    s3Objects.asScala.foreach { x =>

      val strPath = x.getKey.trim
      //Files only, no folders
      if (!strPath.endsWith("/")) {
        val s3Object = (s3BucketName, strPath)
        objectList += s3Object
      }

    }

    objectList
  }

//  def listObjectsNew(
//                   s3Client: AmazonS3 = AmazonS3ClientBuilder.standard().withRegion("ap-southeast-2").build,
//                   s3BucketName: String,
//                   s3Path: String
//                 ): ArrayBuffer[(String, String)] = {
//    var s3PathWrapped = s3Path
//    if (!s3PathWrapped.endsWith("/")) {
//      s3PathWrapped = s3Path + "/"
//    }
//
//    LogUtil.info("s3 list","Listing s3 ")
//    var s3Listing = s3Client.listObjects(s3BucketName, s3PathWrapped)
//    LogUtil.info("s3 list","Listing s3 objects "+s3Listing)
//    val s3Objects = s3Listing.getObjectSummaries
//    //LogUtil.info("s3 list","Listing s3 objects summaries "+s3Objects)
//
//    while (s3Listing.isTruncated) {
//      s3Listing = s3Client.listNextBatchOfObjects(s3Listing)
//      //LogUtil.info("s3 list","Listing s3 objects inside loop"+s3Listing)
//      s3Objects.addAll(s3Listing.getObjectSummaries)
//      //LogUtil.info("s3 list","Listing s3 objects summaries inside loop "+s3Objects)
//    }
//
//    val objectList = new ArrayBuffer[(String, String)]()
//    s3Objects.asScala.foreach { x =>
//      //LogUtil.info("s3 list","s3 path : "+x.getKey.trim)
//      val strPath = x.getKey.trim
//      //Files only, no folders
//      if (!strPath.endsWith("/")) {
//        LogUtil.info("s3 list","s3 path ends without / : "+x.getKey.trim)
//        val s3Object = (s3BucketName, strPath)
//        objectList += s3Object
//      }
//
//    }
//    //LogUtil.info("s3 list","object list : "+objectList)
//    objectList
//  }

  def listObjectsV2(
                     s3Client:AmazonS3 = AmazonS3ClientBuilder.standard().withRegion("ap-southeast-s").build,
                     s3BucketName:String,
                     s3Path:String,
                     startAfter:String = null,
                     takeNumOfFiles:Int = 10000000
                   ): ArrayBuffer[(String,String,Date,Long)] = {

    var s3PathWrapped = s3Path
    if(s3PathWrapped != "" && !s3PathWrapped.endsWith("/")){
      s3PathWrapped = s3Path + "/"
    }

    var request = new ListObjectsV2Request().withBucketName(s3BucketName).withPrefix(s3PathWrapped)
    if(startAfter != null){
      request = new ListObjectsV2Request().withBucketName(s3BucketName).withPrefix(s3PathWrapped).withStartAfter(startAfter)
    }

    var s3Listing = s3Client.listObjectsV2(request)
    val s3Objects = s3Listing.getObjectSummaries
    var nextContinuationToken = s3Listing.getNextContinuationToken

    while(s3Listing.isTruncated && s3Objects.size() < takeNumOfFiles) {
      s3Listing = s3Client.listObjectsV2(request.withContinuationToken((nextContinuationToken)))
      s3Objects.addAll(s3Listing.getObjectSummaries)

      nextContinuationToken = s3Listing.getNextContinuationToken
    }

    val objectList = new ArrayBuffer[(String, String, Date, Long)]()
    s3Objects.asScala.take(takeNumOfFiles).foreach { x =>
      val strPath = x.getKey.trim
      val lastModified = x.getLastModified
      val size = x.getSize

      //Files only, no folders
      if (!strPath.endsWith("/")) {
        val s3Object = (s3BucketName, strPath, lastModified, size)
        objectList += s3Object
      }
    }

    objectList
  }

  //Read S3 file lines to a string
  def readObjectToString(
                          s3Client:AmazonS3 = AmazonS3ClientBuilder.standard().withRegion("ap-southeast-2").build,
                          s3BucketName:String,
                          s3Path:String
                        ): String ={
    val s3Object1 = s3Client.getObject(new GetObjectRequest(s3BucketName, s3Path))
    //    println("S3 Content Type:" + s3Object1.getObjectMetadata.getContentType)
    //    println("S3 Content Length:" + s3Object1.getObjectMetadata.getContentLength)
    //    println("S3 Last Modified:" + s3Object1.getObjectMetadata.getLastModified)
    //    println("S3 SSE Aws Kms Key Id:" + s3Object1.getObjectMetadata.getSSEAwsKmsKeyId)
    val reader = new BufferedReader(new InputStreamReader(s3Object1.getObjectContent))
    val strContent = Stream.continually(reader.readLine()).takeWhile(_ != null).mkString("\r\n")

    strContent
  }


  //Read S3 file lines to an array of lines
  def readObjectToStringLines(
                               s3Client:AmazonS3 = AmazonS3ClientBuilder.standard().withRegion("ap-southeast-2").build,
                               s3BucketName:String,
                               s3Path:String
                             ): ArrayBuffer[String] ={
    val s3Object1 = s3Client.getObject(new GetObjectRequest(s3BucketName, s3Path))
    //    println("S3 Content Type:" + s3Object1.getObjectMetadata.getContentType)
    //    println("S3 Content Length:" + s3Object1.getObjectMetadata.getContentLength)
    //    println("S3 Last Modified:" + s3Object1.getObjectMetadata.getLastModified)
    //    println("S3 SSE Aws Kms Key Id:" + s3Object1.getObjectMetadata.getSSEAwsKmsKeyId)
    val reader = new BufferedReader(new InputStreamReader(s3Object1.getObjectContent))
    val lines = Stream.continually(reader.readLine()).takeWhile(_ != null)
    val arrLines  = ArrayBuffer(lines : _*)

    arrLines
  }


  //Read S3 file lines to an array of attribute map
  def readObjectToAttributeMapLines(
                                     s3Client:AmazonS3 = AmazonS3ClientBuilder.standard().withRegion("ap-southeast-2").build,
                                     s3BucketName:String,
                                     s3Path:String,
                                     attributeDelimiter:String = ","
                                   ): ArrayBuffer[scala.collection.mutable.Map[String, String]] ={
    val fileStringLines = readObjectToStringLines(
      s3Client,
      s3BucketName,
      s3Path
    )

    val attributeMapArray = new ArrayBuffer[scala.collection.mutable.Map[String, String]]()
    val headerArray = fileStringLines(0).split(attributeDelimiter)

    fileStringLines.drop(1).map{x=>
      val fileStringLineSplitArray = x.split(attributeDelimiter)

      val splitMap = scala.collection.mutable.Map[String, String]()
      fileStringLineSplitArray.zipWithIndex.foreach{attribute =>
        splitMap += headerArray(attribute._2) -> attribute._1
      }

      attributeMapArray += splitMap
    }

    attributeMapArray
  }


  //Write string to S3 file
  def writeStringToObject(
                           s3Client:AmazonS3 = AmazonS3ClientBuilder.standard().withRegion("ap-southeast-2").build,
                           s3BucketName:String,
                           s3Path:String,
                           strWrite:String,
                           kmsKeyID:String,
                           s3ACL:String
                         ): Unit ={
    try{
      val metadata = new ObjectMetadata
      metadata.setSSEAlgorithm(SSEAlgorithm.KMS.getAlgorithm)

      val inputStream = new ByteArrayInputStream(strWrite.getBytes(StandardCharsets.UTF_8))
      val putObjectRequest = new PutObjectRequest(s3BucketName, s3Path, inputStream, metadata)
      if(kmsKeyID != null && kmsKeyID.trim != ""){
        putObjectRequest.withSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(kmsKeyID))
      }
      if(s3ACL.trim.toLowerCase == "bucket-owner-full-control"){
        putObjectRequest.withCannedAcl(CannedAccessControlList.BucketOwnerFullControl)
      }

      s3Client.putObject(putObjectRequest)
    } catch{
      case ex:Exception =>
        println(s"==========S3 PutObject Error:==========")
        println(ex.toString)
    }
  }

  //Delete a single S3 file
  def deleteObject(
                    s3Client: AmazonS3 = AmazonS3ClientBuilder.standard().withRegion("ap-southeast-2").build,
                    s3BucketName: String,
                    s3Path: String
                  ): Unit = {
    s3Client.deleteObject(s3BucketName, s3Path)
  }

  //Empty a S3 folder
  def emptyFolder(
                   s3Client: AmazonS3 = AmazonS3ClientBuilder.standard().withRegion("ap-southeast-2").build,
                   s3BucketName: String,
                   s3Path: String
                 ): Unit = {
    val deleteList = S3Util.listObjects(s3Client, s3BucketName, s3Path)
    deleteList.foreach { x =>
      S3Util.deleteObject(s3Client, s3BucketName, x._2)
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  //Upload local file to S3
  def uploadLocalFileToObject(
                               s3Client:AmazonS3 = AmazonS3ClientBuilder.standard().withRegion("ap-southeast-2").build,
                               s3BucketName:String,
                               s3Path:String,
                               filePath:String,
                               kmsKeyID:String,
                               s3ACL:String
                             ): Unit ={
    try{
      val metadata = new ObjectMetadata
      metadata.setSSEAlgorithm(SSEAlgorithm.KMS.getAlgorithm)

      val putObjectRequest = new PutObjectRequest(s3BucketName, s3Path, new FileInputStream(filePath), metadata)
      if(kmsKeyID != null && kmsKeyID.trim != ""){
        putObjectRequest.withSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(kmsKeyID))
      }
      if(s3ACL.trim.toLowerCase == "bucket-owner-full-control"){
        putObjectRequest.withCannedAcl(CannedAccessControlList.BucketOwnerFullControl)
      }

      s3Client.putObject(putObjectRequest)
    } catch{
      case ex:Exception =>
        println(s"==========S3 PutObject Error:==========")
        println(ex.toString)
    }
  }



  //Upload HDFS file to S3 by S3 Client
  def uploadHDFSFileToS3(
                          s3Client:AmazonS3 = AmazonS3ClientBuilder.standard().withRegion("ap-southeast-2").build,
                          s3BucketName:String,
                          s3Path:String,
                          hdfsFilePath:String,
                          kmsKeyID:String,
                          s3ACL:String
                        ): Unit ={
    try{
      val metadata = new ObjectMetadata
      metadata.setSSEAlgorithm(SSEAlgorithm.KMS.getAlgorithm)

      //Initial HDFS input stream
      val hdfsFS = FileSystem.get(new Configuration())
      val hdfsInputStream = hdfsFS.open(new org.apache.hadoop.fs.Path(hdfsFilePath))

      //Upload to S3
      val putObjectRequest = new PutObjectRequest(s3BucketName, s3Path, hdfsInputStream, metadata)
      if(kmsKeyID != null && kmsKeyID.trim != ""){
        putObjectRequest.withSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(kmsKeyID))
      }
      if(s3ACL.trim.toLowerCase == "bucket-owner-full-control"){
        putObjectRequest.withCannedAcl(CannedAccessControlList.BucketOwnerFullControl)
      }

      s3Client.putObject(putObjectRequest)

      //Close HDFS input stream
      hdfsInputStream.close()
      //      hdfsFS.close()
    } catch{
      case ex:Exception =>
        println(s"==========S3 PutObject Error:==========")
        println(ex.toString)
    }
  }

  //Copy from S3 to HDFS
  def copyFromS3ToHDFS(
                        s3AccessKey:String = null,
                        s3SecrectKey:String = null,
                        s3BucketName:String,
                        s3Path:String,
                        hdfsFilePath:String
                      ): Unit ={
    println("==========Start copying from S3 \"" + s3Path + "\" to HDFS \"" + hdfsFilePath + "\"==========")


    val s3Client = initS3Client(
      s3AccessKey,
      s3SecrectKey
    )

    //Initial S3 input stream and HDFS output stream
    val sourceS3Object = s3Client.getObject(new GetObjectRequest(s3BucketName, s3Path))
    val s3InputStream = sourceS3Object.getObjectContent
    val hdfsFS = FileSystem.get(new Configuration())
    val hdfsOutputStream = hdfsFS.create(new org.apache.hadoop.fs.Path(hdfsFilePath))

    val b = new Array[Byte](1024)
    var numBytes = s3InputStream.read(b)
    while(numBytes > 0){
      hdfsOutputStream.write(b, 0, numBytes)
      numBytes = s3InputStream.read(b)
    }

    s3InputStream.close()
    sourceS3Object.close()
    hdfsOutputStream.close()
    //    hdfsFS.close()


    println("==========Finish copying from S3 \"" + s3Path + "\" to HDFS \"" + hdfsFilePath + "\"==========")
  }

  //Copy file from S3 to S3
  def copyFileFromS3ToS3(
                          s3AccessKey1:String = null,
                          s3SecrectKey1:String = null,
                          s3BucketName1:String,
                          s3Path1:String,

                          s3AccessKey2:String = null,
                          s3SecrectKey2:String = null,
                          s3BucketName2:String,
                          s3Path2:String,
                          kmsKeyID2:String,
                          s3ACL2:String
                        ): Unit ={
    println("==========Start copying from S3 \"s3://" + s3BucketName1 + "/" + s3Path1 + "\" to S3 \"s3://" + s3BucketName2 + "/" + s3Path2 + "\"==========")


    val s3Client1 = initS3Client(
      s3AccessKey1,
      s3SecrectKey1
    )

    val s3Client2 = initS3Client(
      s3AccessKey2,
      s3SecrectKey2
    )

    //Initial S3 input stream and S3 output stream
    val sourceS3Object = s3Client1.getObject(new GetObjectRequest(s3BucketName1, s3Path1))
    val s3InputStream = sourceS3Object.getObjectContent

    //Config KMS, ACL and PutObjectRequest
    val metadata = new ObjectMetadata
    metadata.setSSEAlgorithm(SSEAlgorithm.KMS.getAlgorithm)

    val putObjectRequest = new PutObjectRequest(s3BucketName2, s3Path2, s3InputStream, metadata)
    if(kmsKeyID2 != null && kmsKeyID2.trim != ""){
      putObjectRequest.withSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(kmsKeyID2))
    }
    if(s3ACL2.trim.toLowerCase == "bucket-owner-full-control"){
      putObjectRequest.withCannedAcl(CannedAccessControlList.BucketOwnerFullControl)
    }

    //Start copying
    s3Client2.putObject(putObjectRequest)

    //Close streams and objects
    s3InputStream.close()
    sourceS3Object.close()


    println("==========Finish copying from S3 \"s3://" + s3BucketName1 + "/" + s3Path1 + "\" to S3 \"s3://" + s3BucketName2 + "/" + s3Path2 + "\"==========")
  }

}
