# bd201-m06-spark  
  * build project:  
  ```mvn clean package```  
  * copy jar to hdp container:    
  ```docker cp bd201-md06-spark-1.0.0-SNAPSHOT-jar-with-dependencies.jar sandbox-hdp:/home/201bd/md06-spark/```  
  * execute spark:  
  ```spark-submit --class spark.batching.BatchReader /home/201bd/md06-spark/bd201-md06-spark-1.0.0-SNAPSHOT-jar-with-dependencies.jar --deploy-mode cluster```  
  * view saved expedia data partitioned by srch_ci:  
  ```hdfs dfs -ls /tmp/expedia_batch_result```  


