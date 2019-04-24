#!/bin/bash

cd udtf

#build app with udtf
mvn clean package

#create directory in hdfs for udtf jar
hadoop fs -mkdir -p /common/datahive/jar/

#copy udtf jar to hdfs
hadoop fs -copyFromLocal target/parse-ua-udtf-0.0.1-SNAPSHOT-jar-with-dependencies.jar /common/datahive/jar/

#remove old function version
hive -e "DROP FUNCTION IF EXISTS parseUA;"

#create udtf function
hive -e "CREATE FUNCTION parseUA as 'com.epam.bigdata.parse_ua_udtf.ParseUserAgentUADTF' USING JAR 'hdfs:///common/datahive/jar/parse-ua-udtf-0.0.1-SNAPSHOT-jar-with-dependencies.jar';"

#show created function
hive -e "describe function extended parseUA;"
cd ..

