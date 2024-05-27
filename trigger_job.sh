#!/bin/bash
user=${genericUser}
env=${env}
export HADOOP_USER_NAME=${user} 
keytab="${user}.keytab"
principal="${user}@PC.INTERNAL.MACQUARIE.COM"

echo "Creating keytab for user "
source /etc/profile

export KRB5CCNAME=/tmp/${user}
python /arturo/scripts/cyberark/retrieve_passwords.py ${user} 'AP003906' /opt/artifacts/oprisk/ kerberos
export keytabPath=/opt/artifacts/oprisk/keytabs/${user}/${user}.keytab
kinit -kt /opt/artifacts/oprisk/keytabs/${user}/${user}.keytab ${user}

hadoop fs -cat /conf/global.env > global.env
source global.env

pwd
cd ${files_path}/
ls -lR
conf_path=${files_path}/transformation_objects-lib/conf.json
DTS_JAR_PATH_v4=./dl-token-client-4.0.26.jar

echo "Setting output variable "
if ! output="$(java -jar ${files_path}/dl-token-client-4.0.26.jar ${conf_path} 2>&1)"; then
echo "${output}"
exit 1
fi

#output="$(java -jar ${files_path}/dl-token-client-4.0.26.jar ${conf_path} 2>&1)"
output=`echo ${output} | grep -oP '{.*}'`

echo "Setting keys "
export AWS_ACCESS_KEY_ID=`echo ${output} | jq -r .accessKeyId`
export AWS_SECRET_ACCESS_KEY=`echo ${output} | jq -r .secretAccessKey`
export AWS_SESSION_TOKEN=`echo ${output} | jq -r .sessionToken`

echo "Setting java home "
export JAVA_HOME=/usr/java/openjdk_1.8.0_302/
export PATH=$PATH:$JAVA_HOME/bin

#echo "Creating hdfs directory and copying config file in it.."
#hadoop fs -mkdir -p ${baseHdfsPath}/
#hadoop fs -put -f ${files_path}/transformation_objects-lib/transformation_objects.json ${baseHdfsPath}/
#hadoop fs -ls ${baseHdfsPath}/
echo "Copying json file to S3 path"

aws s3 cp ${files_path}/transformation_objects-lib/transformation_objects.json s3://${configFileS3Path}/

today="$(date +'%Y%m%d')"
timestamp="$(date +'%Y%m%d%H%M%S')"
batchId=$timestamp
echo "batchId=$timestamp"


echo "Job Started..."
echo "$user"

if [ -z "$AWS_ACCESS_KEY_ID" ]
then
echo "STS didn't provide any token!! Exiting...."
exit 1
else 

echo "STS token received..submitting Transformation application"

spark-submit --master yarn \
    --deploy-mode cluster \
    --conf spark.acls.enable=true --conf spark.ui.view.acls.groups="acg-cog-for-cdh-developers-rmg" \
    --conf spark.executor.extraClassPath=./ --driver-memory=8g \
    --conf spark.driver.memoryOverhead=1g \
    --conf spark.network.timeout=240000 \
	--conf spark.sql.broadcastTimeout=600000 \
    --driver-cores=4 \
    --num-executors=8 \
    --executor-memory=5g \
    --executor-cores=4 \
    --conf spark.yarn.maxAppAttempts=1 \
	--queue root.nonres.high \
    --conf spark.sql.broadcastTimeout=2000 \
	--conf spark.sql.adaptive.enabled=true \
	--name OP-TransformationObjects-Job \
    --principal ${principal} \
    --keytab /opt/artifacts/oprisk/keytabs/${user}/${user}.keytab \
	--files ${files_path}/transformation_objects-lib/transformation_objects.json \
	--jars ojdbc6-11.2.0.3.jar,dl-token-client-4.0.26.jar \
    --class com.macquarie.rmg.openpages.App openpages-transformation-${openpagesTransformationJarVersion}.jar \
    "jobName=transformation_objects" \
    "batchId=$batchId" \
    "s3AccessKey=$AWS_ACCESS_KEY_ID" \
    "s3SecretKey=$AWS_SECRET_ACCESS_KEY" \
    "s3SessionToken=$AWS_SESSION_TOKEN" \
    "configFileFullPath=s3a://${configFileS3Path}/transformation_objects.json"
	
if [[ $? -ne 0 ]]; then echo "Spark job has failed. Exiting"; exit 1; fi
	
fi
