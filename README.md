# spark-examples
This project does some data-wrangling on a json dataset; answering analytical questions from Udacity's Data Engineering Nanodegree program

## Data-set Schema
root                                                          
    |-- artist: string (nullable = true) <br>
    |-- auth: string (nullable = true) <br>
    |-- firstName: string (nullable = true) <br>
    |-- gender: string (nullable = true) <br>
    |-- itemInSession: long (nullable = true) <br>
    |-- lastName: string (nullable = true) <br>
    |-- length: double (nullable = true) <br>
    |-- level: string (nullable = true) <br>
    |-- location: string (nullable = true) <br>
    |-- method: string (nullable = true) <br>
    |-- page: string (nullable = true) <br>
    |-- registration: long (nullable = true) <br>
    |-- sessionId: long (nullable = true) <br>
    |-- song: string (nullable = true) <br>
    |-- status: long (nullable = true) <br>
    |-- ts: long (nullable = true) <br>
    |-- userAgent: string (nullable = true) <br>
    |-- userId: string (nullable = true) <br>

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

## Question 3: Question 3: How many songs were played form the most played artist?
### Steps:
    - Select and group dataframe by artists
    - Run a `count` aggregate.
    - Sort in descending order and display the first item in the results.