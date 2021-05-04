from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.window import Window

spark_session = SparkSession.builder.appName("Wrangling Data").getOrCreate()

spark_df = spark_session.read.json('data/sparkify_log_small.json')

# print schema
spark_df.printSchema()


print('==================== QUESTION 1: Which page did user id "" not visit? ====================')
all_pages = spark_df.select('page').dropDuplicates()
get_user_pages = spark_df.select('page').where(spark_df['userId'] == "").dropDuplicates()
non_visited_pages = set(all_pages.collect()) - set(get_user_pages.collect())
for row in non_visited_pages:
    print(row.page)


print('==================== Question 2: How many females are in the dataset? ====================')
count_females = spark_df.select('userId').where(spark_df.gender == 'F').dropDuplicates().count()
print(count_females)

print('==================== Question 3: How many songs were played form the most played artist? ====================')
spark_df.select('artist').groupBy('artist').agg({'artist': 'count'}).sort(desc('count(artist)')).show(1)