#!/bin/sh
set -ex

echo ""
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
date


appid=`yarn application -list | grep urlWhiteFilter | awk  'NR==1{print $1}'`

if [ -n "$appid" ];then
	yarn application -kill $appid
	echo "*************last application has been killed!!!!!!!!!************"
else
        echo '*************last application finished successfully.************'
fi


day=$(date  +%Y%m%d)
hour=$(date -d"2 hours ago" +%H)
hdfs dfs -rm -r -f /tmp/nx_url_result_hdfs/$day-$hour/*

cat /home/hyj/rans/whiteFilter.scala | /opt/spark-1.6.2-bin-hadoop2.6/bin/spark-shell --name urlWhiteFilter --master yarn-client --num-executors 8 --executor-memory 20g --executor-cores 3 --driver-memory 20g --driver-cores 3 

sh /home/hyj/rans/copy.sh $day $hour 


date

echo "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
echo ""
