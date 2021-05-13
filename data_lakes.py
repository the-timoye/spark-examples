import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

import sparknlp
sparknlp.start()
from sparknlp.pretrained import PretrainedPipeline

pd.set_option('max_colwidth', 800)
spark = SparkSession.builder.config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:3.0.3").getOrCreate()
spark

data_path = 'data/reddit-worldnews.json'
df = spark.read.json(data_path)

print('dataframe count = {}'.format(df.count()))

title = 'data.title'
author = 'data.author'

print('============== AUTHOR Vs TITLE ==============')
df_author_title = df.select(title, author)
print(df_author_title.limit(10).toPandas())

print('============== WORD COUNT ==============')
df_word_count = df.select(F.explode(F.split(title, '\\s+')).alias("word")).groupBy("word").count().sort(F.desc('count'))
print(df_word_count.limit(20).toPandas())

print('============== ANNOTATED DATAFRAME SCHEMA ==============')
explain_document_pipeline = PretrainedPipeline("explain_document_ml")
df_annotated = explain_document_pipeline.annotate(df_author_title, "title")
df_annotated.printSchema()

print('============== QUERY MAPPEED TYPE SUB-FIELDS ==============')
df_check_data = df_annotated.select(["text", "pos.metadata", "pos.result"])
print(df_check_data.limit(10).toPandas())


# extract POS from the annotated dataframe
df_pos = df_annotated.select(F.explode("pos").alias("pos"))
print(df_pos.toPandas())
df_pos.printSchema()

print('============== VIEW ONLY PROPER NOUNS ==============')
df_pos_nouns = df_pos.where("pos.result = 'NNP' OR pos.result = 'NNPS'")
df_nouns = df_pos_nouns.selectExpr(["pos.metadata['word'] AS word", "pos.result AS part_of_speech"])
print(df_nouns.limit(10).toPandas())

print('============== VIEW MOST USED NOUNS==============')
df_common_nouns = df_nouns.groupBy("word").count().sort(F.desc("count"))
print(df_common_nouns.toPandas())