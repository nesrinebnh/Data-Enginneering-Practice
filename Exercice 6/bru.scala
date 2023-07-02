import org.apache.spark.sql.{SparkSession, DataFrame}

// Create a SparkSession
val spark = SparkSession.builder().appName("CSV Reader").master("local[*]").getOrCreate()

// Read CSV file into a DataFrame
val booksPath = "/user/hadoop/books.csv"
val booksOptions = Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> ";")

val books: DataFrame = spark.read.format("csv").options(booksOptions).load(booksPath)

val usersPath = "/user/hadoop/users.csv"
val usersOptions = Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> ";")

val users: DataFrame = spark.read.format("csv").options(usersOptions).load(usersPath)

val ratingsPath = "/user/hadoop/ratings.csv"
val ratingsOptions = Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> ";")

val ratings: DataFrame = spark.read.format("csv").options(ratingsOptions).load(ratingsPath)

books.show()
users.show()
ratings.show()


// Join dataframes
val joinedDF: DataFrame = books.join(ratings, Seq("ISBN")).join(users, Seq("User-ID"))
val results: DataFrame = joinedDF.select("Book-Title","Year-Of-Publication","Publisher","Age","Book-Rating")

// Remove rows where year of publication is equal to zero
val resultsFiltered: DataFrame = results.filter(col("Year-Of-Publication") =!= 0)

// Retrieve books with best ratings
val best_books: DataFrame = resultsFiltered.groupBy("Book-Title").agg(avg("Book-Rating").alias("Avg-Rating"),max("Year-Of-Publication").alias("Year-Of-Publication")).orderBy(col("Avg-Rating").desc, col("Year-Of-Publication"))
best_books.show()
