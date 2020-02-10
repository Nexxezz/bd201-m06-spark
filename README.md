# bd201-m06-spark-batching
* Read hotels&weather data from Kafka with Spark application in a batch manner (by specifying start offsets and batch limit in configuration file).  
    * build project:  
     ```mvn clean package```  
     * copy jar to hdp container:  
     ```docker cp bd201-md06-spark-1.0.0-SNAPSHOT-jar-with-dependencies.jar sandbox-hdp:/home/201bd/md06-spark/```  
     * execute spark application with spark-submnit:  
     ```spark-submit --class spark.batching.BatchReader  
     /home/201bd/md06-spark/bd201-md06-spark-1.0.0-SNAPSHOT-jar-with-dependencies.jar  
     --deploy-mode cluster```
* Read Expedia data from HDFS with Spark.
    * see comments in BatchReader class
* Calculate idle days (days betweeen current and previous check in dates) for every hotel.  
    * see comments in BatchReader class
* Validate data:  
    * Remove all booking data for hotels with at least one "invalid" row (with idle days more than or equal to 2 and less than 30).  
        * see comments in BatchReader class
    * Print hotels info (name, address, country etc) of "invalid" hotels and make a screenshot. Join expedia and hotel data for this purpose.  
        * see screenshot 1  
    * Group the remaining data and print bookings counts: 1) by hotel country, 2) by hotel city. Make screenshots of the outputs  
        * see screenshot 2  
* Store "valid" Expedia data in HDFS partitioned by year of "srch_ci".  
    * view saved expedia data partitioned by srch_ci:  
    ```hdfs dfs -ls /tmp/expedia_batch_result```  

# bd201-m06-spark-streaming
* Read Expedia data for 2016 year from HDFS as initial state DataFrame. Read data for 2017 year as streaming data  
    * see comments in StreamReader clazz
* Enrich both DataFrames with weather: add average day temperature at checkin (join with hotels+weaher data from Kafka topic)  
    * see comments in StreamReader clazz
* Filter incoming data by having average temperature more than 0 Celsius degrees.  
    * see comments in StreamReader clazz
* Store final data in HDFS
    * see comments in StreamReader clazz
