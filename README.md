# hive_on_spark_sample
hive on spark simple example

将s1数据上传到hdfs上面:

        ./bin/spark-submit --class SimpleApp --master yarn-cluster --num-executors 4 --executor-cores 1 --driver-memory 512m --executor-memory 512m ../myhive_2.10-1.0.jar  s1  ./q5.sql 


