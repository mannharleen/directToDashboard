#
# If running using EMR
# 
## SCP jar files to cluster
scp -i .ssh/EC2-bigtapp-key.pem /c/Users/harleen.singh/IdeaProjects/directToDashboard/target/scala-2.11/directtodashboard_2.11-0.1.jar hadoop@10.30.7.170:~
scp -i .ssh/EC2-bigtapp-key.pem /c/Users/harleen.singh/IdeaProjects/directToDashboard/lib/* hadoop@10.30.7.170:~

## SCP config file to cluster
scp -i .ssh/EC2-bigtapp-key.pem /c/Users/harleen.singh/IdeaProjects/directToDashboard/src/main/Resources/application.conf hadoop@10.30.7.170:~

## Login into cluster (optional)
ssh -i .ssh/EC2-bigtapp-key.pem hadoop@10.30.7.170

## Remove any hadoop files in the relevant hdfs folder
hdfs dfs -rm -R -f /home/hadoop/
hdfs dfs -ls /home/

## Spark submit 
spark-submit --master yarn --deploy-mode client --class com.pp.mod.directToDashboard --name d2d --jars /home/hadoop/mssql-jdbc-6.1.0.jre8.jar,/home/hadoop/mysql-connector-java-5.1.38.jar,/home/hadoop/ojdbc6.jar,/home/hadoop/postgresql-42.2.1.jar --packages com.typesafe:config:1.3.1 --driver-java-options "-Dconfig.file=\/home\/hadoop\/application.conf" --driver-memory 2G --executor-memory 10G --executor-cores 6 directtodashboard_2.11-0.1.jar

#
# If running using AWS CLI EMR 
#
aws emr create-cluster --applications Name=Hadoop Name=Hive Name=Spark Name=Ganglia --tags 'Project=bigdata' --ec2-attributes '{"KeyName":"EC2-bigtapp-key","AdditionalSlaveSecurityGroups":["sg-8566c6e3"],"InstanceProfile":"AdminAccess","ServiceAccessSecurityGroup":"sg-d666c6b0","SubnetId":"subnet-8718fae0","EmrManagedSlaveSecurityGroup":"sg-c365c5a5","EmrManagedMasterSecurityGroup":"sg-3366c655","AdditionalMasterSecurityGroups":["sg-74872012"]}' --release-label emr-5.11.0 --log-uri 's3n://datalake-uat/' --instance-groups '[{"InstanceCount":2,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"CORE","InstanceType":"m4.xlarge","Name":"slave"},{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"MASTER","InstanceType":"m4.xlarge","Name":"master"}]' --bootstrap-actions '[{"Path":"s3://datalake-uat/test/emr_bootstrap_java_8.sh","Name":"boot_java8"}]' --no-visible-to-all-users --ebs-root-volume-size 10 --service-role EMR_DefaultRole --enable-debugging --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region ap-southeast-1 --name 'emr-dev-harleen-27-2-18' --configurations '[{"Classification": "capacity-scheduler","Properties": {"yarn.scheduler.capacity.resource-calculator": "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"}}]' --steps Type=CUSTOM_JAR,Name=CustomJAR,ActionOnFailure=CONTINUE,Jar=s3://ap-southeast-1.elasticmapreduce/libs/script-runner/script-runner.jar,Args=["s3://modharleen/directToDashboard/run.sh"]
#
# If running using Data pipeline
#
### copy all relevant files to s3://modharleen/directToDashboard/
## EMR step (for Data pipeline)
s3://ap-southeast-1.elasticmapreduce/libs/script-runner/script-runner.jar,s3://modharleen/directToDashboard/run.sh





// // command-runner.jar,spark-submit,--master,yarn,--class,com.yourcompany.yourpackage.YourClass,s3://modharleen/ojdbc6.jar
-- this one not working: 
/*
spark-submit --master yarn --deploy-mode cluster --class com.parkwaypantai.mod.directToDashboard --name d2d --jars /home/hadoop/mssql-jdbc-6.1.0.jre8.jar,/home/hadoop/mysql-connector-java-5.1.38.jar,/home/hadoop/ojdbc6.jar,/home/hadoop/postgresql-42.2.1.jar --packages com.typesafe:config:1.3.1 --driver-java-options "-Dconfig.file=\/home\/hadoop\/application.conf" --files /home/hadoop/application.conf --driver-memory 2G --executor-memory 10G --executor-cores 6 --driver-cores 6 directtodashboard_2.11-0.1.jar
*/