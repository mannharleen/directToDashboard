
## SCP jar files to cluster
scp -i .ssh/EC2-bigtapp-key.pem /c/Users/harleen.singh/IdeaProjects/directToDashboard/target/scala-2.11/directtodashboard_2.11-0.1.jar hadoop@10.29.7.220:~
scp -i .ssh/EC2-bigtapp-key.pem /c/Users/harleen.singh/IdeaProjects/directToDashboard/lib/* hadoop@10.29.7.220:~
## SCP config file to cluster
scp -i .ssh/EC2-bigtapp-key.pem /c/Users/harleen.singh/IdeaProjects/directToDashboard/src/main/Resources/application.conf hadoop@10.29.7.220:~

## Login into cluster (optional)
ssh -i .ssh/EC2-bigtapp-key.pem hadoop@10.29.7.220

## Remove any hadoop files in the relevant hdfs folder
hdfs dfs -rm -R -f /home/hadoop/
hdfs dfs -ls /home/

## Spark submit 
spark-submit --master yarn --deploy-mode client --class com.pp.mod.directToDashboard --name d2d --jars /home/hadoop/mssql-jdbc-6.1.0.jre8.jar,/home/hadoop/mysql-connector-java-5.1.38.jar,/home/hadoop/ojdbc6.jar,/home/hadoop/postgresql-42.2.1.jar --packages com.typesafe:config:1.3.1 --driver-java-options "-Dconfig.file=\/home\/hadoop\/application.conf" --driver-memory 2G --executor-memory 10G --executor-cores 6 directtodashboard_2.11-0.1.jar


-- this one not working: 
/*
spark-submit --master yarn --deploy-mode cluster --class com.parkwaypantai.mod.directToDashboard --name d2d --jars /home/hadoop/mssql-jdbc-6.1.0.jre8.jar,/home/hadoop/mysql-connector-java-5.1.38.jar,/home/hadoop/ojdbc6.jar,/home/hadoop/postgresql-42.2.1.jar --packages com.typesafe:config:1.3.1 --driver-java-options "-Dconfig.file=\/home\/hadoop\/application.conf" --files /home/hadoop/application.conf --driver-memory 2G --executor-memory 10G --executor-cores 6 --driver-cores 6 directtodashboard_2.11-0.1.jar
*/