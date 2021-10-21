from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

def parse_data_from_kafka_message(sdf, schema):
    assert sdf.isStreaming == True, "DataFrame doesn't receive streaming data"
    
    sdf = sdf.withColumn("value", from_json("value", schema)) \
        .select(
            "timestamp",
            col('value.*')
        ) \
        .withColumn(
            "content", 
            lower(col("content"))
        ) 
        
    return sdf
    
if __name__ == "__main__":

    spark = (SparkSession.builder
            .master("local[*]") \
            .config('spark.executor.memory', '8g') \
            .config('spark.driver.memory', '8g') \
            .appName('sg.edu.smu.is459.assignment3') \
            .getOrCreate())

    #Read from Kafka's topic scrapy-output
    df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "scrapy-output") \
            .option("startingOffsets", "earliest") \
            .load()

    #Parse the fields in the value column of the message
    lines = df.selectExpr("CAST(value AS STRING)", "timestamp")

    #Specify the schema of the fields
    hardwarezoneSchema = StructType([ \
        StructField("topic", StringType()), \
        StructField("author", StringType()), \
        StructField("content", StringType()) \
        ])

    #Use the function to parse the fields
    lines = parse_data_from_kafka_message(lines, hardwarezoneSchema)
    
    
    windowedAuthorCount = lines \
        .withWatermark("timestamp", "2 minutes") \
        .groupBy(
            window("timestamp", "2 minutes", "1 minutes"),
            "author"
        ) \
        .count() \
        .orderBy(desc("window"), desc("count")) \
        .limit(10) \
        .writeStream \
        .queryName("countAuthor") \
        .outputMode("complete") \
        .format("console") \
        .trigger(processingTime="1 minutes") \
        .option("checkpointLocation", "/home/amazinglance/authorCount-checkpoint") \
        .option("truncate", False) \
        .start()
    
    windowedWordCount = lines \
        .withWatermark("timestamp", "2 minutes") \
        .select(
            "timestamp",
            explode(
                split("content", " ")
            ).alias("word")
        ) \
        .groupBy(
            window("timestamp", "2 minutes", "1 minutes"),
            "word"
        ) \
        .count() \
        .orderBy(desc("window"), desc("count")) \
        .limit(10) \
        .writeStream \
        .queryName("countWord") \
        .outputMode("complete") \
        .format("console") \
        .trigger(processingTime="1 minutes") \
        .option("checkpointLocation", "/home/amazinglance/wordCount-checkpoint") \
        .option("truncate", False) \
        .start()
        
    windowedWordCount.awaitTermination()
    windowedAuthorCount.awaitTermination()