# Assignment 2

## Introduction

This assignment leverage on the scraper that was created in Assignment 1 and modified to push posts to __Apache Kafka__ to be then analyse by __Spark Streaming__.


## Environment Setup

To run the program you will need the following installed on your host machine

1) Python 3 or later
2) Apache Kafka
3) Spark

<br>

To install all the Python libraries required, run the following command
```
pip3 install -r requirements.txt
```

## Submitting the Spark Job

Run the following command to submit the spark job

```
>>> spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 kafka_wordcount.py
```