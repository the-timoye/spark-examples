from pyspark.sql import SparkSession

spark_session = SparkSession.builder.appName("Wrangling Data").getOrCreate()

spark_df = spark_session.read.json('data/sparkify_log_small.json')

# print schema
spark_df.printSchema()


# QUESTION 1: Which page did user id "" not visit?
all_pages = spark_df.select('page').dropDuplicates()
get_user_pages = spark_df.select('page').where(spark_df['userId'] == "").dropDuplicates()
non_visited_pages = set(all_pages.collect()) - set(get_user_pages.collect())
for row in non_visited_pages:
    print(row.page)