from pyspark.sql import SparkSession

spark_session = SparkSession.builder.appName('Another App name').getOrCreate()
data_set = spark_session.read.json('data/sparkify_log_small.json')
data_set.createOrReplaceTempView('sparkify_log')

# Question 1: Which page did user id ""(empty string) NOT visit?
all_pages = spark_session.sql('''
    SELECT DISTINCT page
    FROM sparkify_log
''')
user_pages = spark_session.sql( '''
    SELECT DISTINCT page
    FROM sparkify_log
    WHERE userId = ""
''' )

all_pages_list = all_pages.toPandas()['page'].tolist()
user_pages_list = user_pages.toPandas()['page'].tolist()

for page in all_pages_list:
    if page not in user_pages_list:
        print(page)
    continue


# Question 2: How many female users do we have in the data set?

female_users = spark_session.sql( '''
    SELECT gender, COUNT(gender) AS gender_count
    FROM (
        SELECT DISTINCT userId, gender
        FROM sparkify_log
    )
    WHERE gender = 'F'
    GROUP BY gender
''' )
female_users.show()

# Question 3: How many songs were played from the most played artist?

most_played_artist = spark_session.sql('''
    SELECT artist, COUNT(artist) AS artist_count
    FROM sparkify_log
    GROUP BY artist
    ORDER BY artist_count DESC
    LIMIT 1
''')
most_played_artist.show()

# Question 4: How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.