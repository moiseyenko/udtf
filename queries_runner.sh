#!/bin/bash

#create all tables
hive -f queries/create_tables.q

echo "***********************************************************************"
echo "*****************Start insertion in table: city ***********************"
echo "***********************************************************************"
hive -e "insert into table city select id, name from citysrc"

insert_into_partition_table()
{
  table=$1
  echo "***********************************************************************"
  echo "*****************Start insertion in table: $table**********************"
  echo "***********************************************************************"
  for i in 1 2 3
  do
    hive -e "insert into table $table partition (season=$i) select user_agent, cityid from s$i"
  done
}

insert_into_table()
{
  table=$1
  echo "***********************************************************************"
  echo "*****************Start insertion in table: $table**********************"
  echo "***********************************************************************"
  for i in 1 2 3
  do
    hive -e "insert into table $table select user_agent, cityid, $i from s$i"
  done
}

insert_into_partition_table ua_partition
insert_into_partition_table ua_partition_bucketed
insert_into_partition_table uatxf

insert_into_table ua_raw
insert_into_table ua_bucketed

query="queries/finalquery.q"

run_query(){
  while [ "$#" -gt 0 ]
  do
    echo "***********************************************************************"
    echo "*****************Start query with table: $1 ***************************"
    echo "***********************************************************************"
    sed -i 's/#/'$1'/g' $query
    hive -f $query
    sed -i 's/'$1'/#/g' $query
    shift
  done
}

run_query ua_raw ua_bucketed ua_partition ua_partition_bucketed uatxf

echo "***********************************************************************"
echo "*****************Start query with compression *************************"
echo "***********************************************************************"
sed -i 's/#/uatxf/g' $query
hive –hiveconf hive.exec.compress.intermediate=true hive.intermediate.compression.codec='org.apache.hadoop.io.compress.SnappyCodec' -f $query
sed -i 's/uatxf/#/g' $query

echo "***********************************************************************"
echo "*****************Build indexes*************** *************************"
echo "***********************************************************************"
hive -f queries/create_index.q
run_query uatxf

echo "***********************************************************************"
echo "*****************Start query with vectorization ***********************"
echo "***********************************************************************"
sed -i 's/#/uatxf/g' $query
hive –hiveconf hive.vectorized.execution.enabled=true -f $query
sed -i 's/uatxf/#/g' $query
hive -f queries/drop_index.q

echo "***********************************************************************"
echo "************************** FINISH *************************************"
echo "***********************************************************************"



