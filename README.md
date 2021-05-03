# spark-examples
This project does some data-wrangling on a json dataset; answering analytical questions from Udacity's Data Engineering Nanodegree program

## Data-set Schema
root                                                          
    |-- artist: string (nullable = true)
    |-- auth: string (nullable = true)
    |-- firstName: string (nullable = true)
    |-- gender: string (nullable = true)
    |-- itemInSession: long (nullable = true)
    |-- lastName: string (nullable = true)
    |-- length: double (nullable = true)
    |-- level: string (nullable = true)
    |-- location: string (nullable = true)
    |-- method: string (nullable = true)
    |-- page: string (nullable = true)
    |-- registration: long (nullable = true)
    |-- sessionId: long (nullable = true)
    |-- song: string (nullable = true)
    |-- status: long (nullable = true)
    |-- ts: long (nullable = true)
    |-- userAgent: string (nullable = true)
    |-- userId: string (nullable = true)

## Question 1: Which page did user id "" not visit?
### Steps:
    - Unique Pages are selected from the dataset using the `dropDuplicates()` method
    - Unique pages visited by the user is selected from the dataframe using the `where()` and `dropDuplicates()` methods
    - results are put in a set, and missing pages from the users pages are extracted

## Question 2: How many females are in the dataset?
### Steps:
    - `select` all distinct users using the dropDuplicates() method,
    - `filter` by gender 'F'
    - `count` the result