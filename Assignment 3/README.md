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

## NLTK Stopword

To ensure that you have the stopwords install. You would need to run the following code using Python interactive shell before running the actual `assignment2.py`

```
>>> import nltk
>>> nltk.download("stopwords")
```

