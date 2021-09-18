import pyspark
import helper
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, col
from graphframes import *

spark = (SparkSession.builder
    .master("local[*]") \
    .config('spark.executor.memory', '8g') \
    .config('spark.driver.memory', '8g') \
    .appName('sg.edu.smu.is459.assignment2') \
    .getOrCreate())

# Load data
posts_df = spark.read.load('/home/amazinglance/parquet-input/hardwarezone_lance.parquet')

posts_df = posts_df.na.drop()
author_df = posts_df.select('author').distinct()

print('Author number :' + str(author_df.count()))

# Assign ID to the users
author_id = author_df.withColumn('id', monotonically_increasing_id())
author_id.show()

# Construct connection between post and author
left_df = posts_df.select('topic', 'author') \
    .withColumnRenamed("topic","ltopic") \
    .withColumnRenamed("author","src_author")

right_df =  left_df.withColumnRenamed('ltopic', 'rtopic') \
    .withColumnRenamed('src_author', 'dst_author')

#  Self join on topic to build connection between authors
author_to_author = left_df. \
    join(right_df, left_df.ltopic == right_df.rtopic) \
    .select(left_df.src_author, right_df.dst_author) \
    .distinct()
edge_num = author_to_author.count()
print('Number of edges with duplicate : ' + str(edge_num))

# Convert it into ids
id_to_author = author_to_author \
    .join(author_id, author_to_author.src_author == author_id.author) \
    .select(author_to_author.dst_author, author_id.id) \
    .withColumnRenamed('id','src')

id_to_id = id_to_author \
    .join(author_id, id_to_author.dst_author == author_id.author) \
    .select(id_to_author.src, author_id.id) \
    .withColumnRenamed('id', 'dst')

id_to_id = id_to_id.filter(id_to_id.src >= id_to_id.dst).distinct()

id_to_id.cache()

print("Number of edges without duplciate :" + str(id_to_id.count()))

# Build graph with RDDs
graph = GraphFrame(author_id, id_to_id)

# For complex graph queries, e.g., connected components, you need to set
# the checkopoint directory on HDFS, so Spark can handle failures.
# Remember to change to a valid directory in your HDFS
spark.sparkContext.setCheckpointDir('/home/amazinglance/spark-checkpoint')

# Assingment Starts From Here

# Connect Communites
communities = graph.connectedComponents()
communities.show()

# Group users into community and count the amount of user in them
communities.groupBy("component").count().sort(col("count").desc()).show()

# Map Triangle going thru each user
result_triangle = graph.triangleCount()

author_triangle_component = result_triangle \
    .join(communities, communities.id == result_triangle.id) \
    .select(result_triangle.id, result_triangle.author, communities.component, result_triangle["count"])
    
author_triangle_component_rdd = author_triangle_component.rdd

# Map Community to Triangle count and count the average
author_triangle_component_rdd = author_triangle_component_rdd \
    .map(lambda x: (x[2], (x[3], 1))) \
    .reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1])) \
    .map(lambda x: (x[0], x[1][0]/x[1][1]))
    
author_triangle_component_df = spark.createDataFrame(author_triangle_component_rdd, ["author", "count"])
author_triangle_component_df.sort("count", ascending=False).show()

# Finding Frequent Words used by the Community
posts_df.createOrReplaceTempView("posts")
author_post_df = posts_df.select('author', 'content')

author_post_rdd = author_post_df.rdd

author_post_rdd = author_post_rdd \
    .filter( lambda x: x.content != "" ) \
    .groupByKey() \
    .mapValues(list) \
    .map( lambda x: ( x[0], ' '.join(x[1]).lower() ) ) \
    .map( lambda x: ( x[0], helper.remove_urls(x[1]) ) ) \
    .map( lambda x: ( x[0], helper.expandContractions(x[1]) ) ) \
    .map( lambda x: ( x[0], helper.remove_stopwords(x[1]) ) ) \
    .map( lambda x: ( x[0], helper.remove_punctuation(x[1]) ) ) \
    .map( lambda x: ( x[0], x[1].split())) \
    .flatMapValues( lambda x: x ) \
    .map( lambda x: ( (x[0], x[1]), 1)) \
    .reduceByKey( lambda x, y: x + y ) \
    .map(lambda x: (x[0][0], x[0][1], x[1]))
    # .map( lambda x: ( x[0], helper.correct_spelling(x[1]) ) ) \
    
author_post_df = spark.createDataFrame(author_post_rdd, ["author", "word", "count"])

community_author_word_df = communities \
    .join(author_post_df, author_post_df.author == communities.author) \
    .select(communities.component, author_post_df.word, author_post_df["count"])
    
community_author_word_rdd = community_author_word_df.rdd

community_author_word_rdd = community_author_word_rdd \
    .map(lambda x: ( (x[0], x[1]), x[2])) \
    .filter(lambda x: x[0][1] not in helper.generate_stop_list()) \
    .reduceByKey( lambda x, y: x + y ) \
    .map(lambda x: (x[0][0], x[0][1], x[1]))
    
community_author_word_df = spark.createDataFrame(community_author_word_rdd, ["community", "word", "count"])
community_author_word_sorted = community_author_word_df.orderBy(["community", "count"], ascending=[1, 0])
community_author_word_sorted.show()