package com.macquarie.rmg.openpages

import scala.collection.mutable.Map


case class DerivedColumn(
                          var colExpression:String        = null,
                          var colSource:String            = null,
                          var colSourceExpression:String  = null,
                          var colAlias:String             = null
                        )

case class SourceJoinTable(
                            var sourcePath:String                                    = null,
                            var viewName:String                                      = null,
                            var cacheFlag:String                                     = null
)

case class Lookup(
                            var lookupListName:String                                         = null,
                            var lookupListType:String                                         = null,
                            var lookupListSourcePath:String                                   = null,
                            var lookupListSourceSQL:String                                    = null
                          )


case class LookupValue(
                           lookupTableList:Map[String, Any]
                         )
                         
case class StepConfig__Transformation(
                                  var stepCategory:String                       = null,


                                  var numOfWriteRepartition:Int                 = 20,

                                  var hiveTableName:String                      = null,
                                  var hiveTableFullName:String                  = null,
                                  var hiveTableS3Path:String                    = null,
                                  var hiveTableS3FullPath:String                = null,
                                  var s3CheckpointPath: String                  = null,
                                  var flagSchemaChanged:Boolean                 = false,
                                  var sourceJoinSQL:String                      = null,

                                  var preDerivedColumnList:Seq[DerivedColumn]   = Seq[DerivedColumn](),
                                  var postDerivedColumnList:Seq[DerivedColumn]  = Seq[DerivedColumn](),

                                  var partitionColumnList:Seq[String]           = Seq[String](),

                                  var sourceJoinTableList:Seq[SourceJoinTable]  = Seq[SourceJoinTable](),
                                  var lookupList:Seq[Lookup]                    = Seq[Lookup](),
                                  var businessEffeTs:Long                       = 0L
                                )


case class JobConfig(
                      var flagTestLocally:Boolean                               = false,

                      var configMap_JobConfig:Map[String, String]               = Map[String, String](),
                      var jobName:String                                        = null,
                      var batchId:Long                                          = 0L,
                      var configFileFullPath:String                             = null,
                      var s3AccessKey:String                                    = null,
                      var s3SecretKey:String                                    = null,
                      var s3SessionToken:String                                 = null,

//                      var jassFilePath:String                                   = null,
//                      var impalaHostname:String                                 = null,
//                      var impalaPort:String                                     = null,
//                      var impalaFQDNHost:String                                 = null,
//                      var realm:String                                          = null,

                      var projectName:String                                    = null,
                      var s3Bucket:String                                       = null,
                      var s3ProjectFolder:String                                = null,
                      var projectDB:String                                      = null,
                      var jobLogTableName:String                                = null,
                      var jobLogTableFullName:String                            = null,
                      var jobLogTableS3FullPath:String                          = null,
                      var transformationParallelism:Int                          = 1,
                      var transformationMaxWaitTimeInMinutes:Int                 = 600,
                      var jobType: String                                       = null,
                      var sourceJoinSnapshotTablesList: List[String]                  = null,

                      var stepConfigList__Transformation:Seq[StepConfig__Transformation]  = Seq[StepConfig__Transformation]()
//                      var configMap__Impala:Map[String, String]       = Map[String, String]()
                    )


case class RelationshipsTable(
                        batch_id:Long,
                        table_name:String, //?????TODO
                        prv_pid:String,
                        lst_pid:String,
                        rec_cnt:Long,
                        status:String,
                        update_ts:String
                      )
