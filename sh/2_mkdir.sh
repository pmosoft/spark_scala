#######################
# Local
#######################
cd /home/str
mkdir app
mkdir app/str
mkdir app/str/bin
mkdir app/str/data
mkdir app/str/data/entity
mkdir app/str/sh
mkdir app/str/src
mkdir data
chmod 777 -R /home/str

#######################
# HDFS
#######################
su hdfs
hadoop fs -mkdir /user/str
hadoop fs -chmod 777 /user/str
hadoop fs -chown str:str /user/str

su str
hadoop fs -mkdir -p /user/str/entity/parquet
hadoop fs -mkdir -p /user/str/entity/hive

