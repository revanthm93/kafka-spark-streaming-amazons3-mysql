from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row
import os, json

jars = os.path.join(os.pardir, "jars", 'mysql-connector-java-5.1.49.jar')


def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession.builder.config(conf=sparkConf) \
            .config("spark.jars", jars).getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def process(time, rdd):
    print("========= %s =========" % str(time))
    # Get the singleton instance of SparkSession
    spark = getSparkSessionInstance(rdd.context.getConf())
    # Filter if the RDD/Dstream is Empty
    if not rdd.isEmpty():
        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda w: Row(tweetscountbyuser=w))
        wordsDataFrame = spark.createDataFrame(rowRdd)
        # Creates a temporary view using the DataFrame
        wordsDataFrame.createOrReplaceTempView("tweets")
        # Do word count on table using SQL and print it
        wordCountsDataFrame = spark.sql("select * from tweets")
        wordCountsDataFrame.show()
        # write streaming data in batches to mysql db table
        wordCountsDataFrame.write.format('jdbc'). \
            options(url='jdbc:mysql://localhost:3306/twitter?autoReconnect=true&useSSL=false', \
                    driver='com.mysql.jdbc.Driver', dbtable='tweets', user='root', password='admin').mode(
            'append').save()
        # Write streaming data in batches to s3
        wordCountsDataFrame.coalesce(1).write.mode("append").parquet("s3a://kafka-stream-twitter-data/data")


if __name__ == "__main__":
    # Create Spark Context to Connect Spark Cluster
    sc = SparkContext(appName="PythonStreamingKafkaTweetCount")

    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
    # sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIATQKHMLMZ2RHL7JEZ")
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")

    # Set the Batch Interval is 100 sec of Streaming Context
    ssc = StreamingContext(sc, 100)

    # Create Kafka Stream to Consume Data Comes From Twitter Topic
    # localhost:2181 = Default Zookeeper Consumer Address

    kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'twitter': 1})

    # Parse Twitter Data as json
    parsed = kafkaStream.map(lambda v: json.loads(v[1]))
    # Get the Username of the person who tweeted about corona
    user_counts = parsed.map(lambda tweet: (tweet['user']["screen_name"]))
    # Print the User tweet counts
    user_counts.pprint()
    user_counts.foreachRDD(process)

    # Start Execution of Streams
    ssc.start()
    ssc.awaitTermination()
