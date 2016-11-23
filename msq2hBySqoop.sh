#!/bin/bash
tables=(c_record ftp_pathinfo l_file_info l_url_info log_black loginuser m_feature_level m_interface_config)

for tb in ${tables[*]}
do
  echo ""
  echo `date "+%Y-%m-%d %H:%M:%S"`">>>>>>>>>>>>>>>>>>>>  $tb"
  echo "sqoop import -connect jdbc:mysql://22.22.22.22:3306/chanct --username sqoop --password sqoop  -table $tb -split-by id -hive-import --hive-database chanctnew"

  sqoop import -connect jdbc:mysql://22.22.22.22:3306/chanct --username sqoop --password sqoop  -table $tb -split-by id -hive-import --hive-database chanctnew
  echo ""
  echo ""
done
