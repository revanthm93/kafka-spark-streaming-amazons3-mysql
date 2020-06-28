# Twitter and Spark Streaming with Apache Kafka and load the result to Amazon S3 & MySQL database in minor batches. This project is done as part of interview task given by RightData.

This project captures the username data who tweets about corona in real-time. <br>

The captured data is then written to Amazon S3 bucket and as well as MySQL database in real-time as minor batches.

## Code Explanation

1. Authentication operations were completed with Tweepy module of Python.
2. StreamListener named KafkaPushListener was create for Twitter Streaming. StreamListener produces data for Kafka Consumer.
3. Producing data was filtered about CORONA.
4. SparkContext was created to connect Spark Cluster.
5. Kafka Consumer that consumes data from 'twitter' topic was created.
6. Filter the data to get usernames alone and writes the result to Amazon S3 bucket and MYSQL DB.

## Steps to Run the Project:

1.Create Twitter API account and get keys and update it in code.

2.Start ZOOKEEPER:



	C:\Users\revam\kafka_2.13-2.5.0\bin\windows\zookeeper-server-start.bat C:\Users\revam\kafka_2.13-2.5.0\config/zookeeper.properties
	


3.Start Apache Kafka:



	C:\Users\revam\kafka_2.13-2.5.0\bin\windows\kafka-server-start.bat C:\Users\revam\kafka_2.13-2.5.0\config/server.properties



4.Start "kafka_push_listener.py" with will start producing data for Kafka Consumer:



	python C:\Users\revam\PycharmProjects\kafka-spark-streaming-amazons3-mysql\src\kafka_push_to_topic.py



5.Start "kafka_spark-s3-mysql.py" will start consuming data, read, process and transform it with spark and processed data is written to Amazon S3 & MYSQL DB.



	spark-submit --jars C:\Users\revam\PycharmProjects\kafka-spark-streaming-amazons3-mysql\jars\mysql-connector-java-5.1.49.jar --driver-class-path C:\Users\revam\PycharmProjects\kafka-spark-streaming-amazons3-mysql\jars\mysql-connector-java-5.1.49.jar --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,com.amazonaws:aws-java-sdk-pom:1.11.123,org.apache.hadoop:hadoop-aws:2.7.3 C:\Users\revam\PycharmProjects\kafka-spark-streaming-amazons3-mysql\src\kafka_spark-s3-mysql.py


