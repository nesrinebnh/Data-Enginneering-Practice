import org.apache.spark.sql.{SparkSession, DataFrame}


// Create a SparkSession
val spark = SparkSession.builder().appName("Movie analyses").master("local").getOrCreate()

// Read CSV file into a DataFrame
val csvPath = "/user/hadoop/movies.csv"
val csvOptions = Map("header" -> "true", "inferSchema" -> "true", "delimiter"->",")

val movies_df: DataFrame = spark.read.format("csv").options(csvOptions).load(csvPath)

// Show the DataFrame
movies_df.show()

// Read CSV file into a DataFrame
val csvPath = "/user/hadoop/ratings.csv"
val csvOptions = Map("header" -> "true", "inferSchema" -> "true", "delimiter"->",")

val ratings_df: DataFrame = spark.read.format("csv").options(csvOptions).load(csvPath)

// Show the DataFrame
ratings_df.show()

// Count the number of unique movies and unique users.
val movies_count = movies_df.select("movieId").distinct().count()
println(s"the number of unique movies is: $movies_count")

val users_count = ratings_df.select("userId").distinct().count()
println(s"the number of unique users is $users_count")

// Calculate the average rating for each movie.
val avg_rating = ratings_df.groupBy("movieId").agg(avg(col("rating")).alias("avg_ratings"))
println(" the average rating for each movie")
avg_rating.show()

// Find the top-rated movies based on average rating.
val top_10_rated_movies: DataFrame = avg_rating.orderBy(col("avg_ratings").desc).limit(10).join(movies_df,"movieId").select("title", "avg_ratings")
top_10_rated_movies.show()

// Determine the number of ratings per user and identify the users with the most ratings.
val number_ratings_user: DataFrame = ratings_df.groupBy("userId").agg(sum(col("rating")).alias("number_ratings")).orderBy(col("number_ratings").desc)
number_ratings_user.show()

// Join the movie and ratings DataFrames based on the movie ID
val movies_ratings_df: DataFrame = movies_df.join(ratings_df, Seq("movieId"), "inner")
movies_ratings_df.show()


// Calculate the average rating per genre
val genres_df: DataFrame = movies_ratings_df.withColumn("genres_array", split(col("genres"), "\\|"))
val exploded_genres_df: DataFrame = genres_df.select(col("movieId"), col("title"), explode(col("genres_array")).as("genre"), col("rating"))
exploded_genres_df.show()
val avg_rating_genre: DataFrame = exploded_genres_df.groupBy("genre").agg(avg("rating").alias("avg_rating"))
avg_rating_genre.show()

// Save the results of the analysis to CSV files
avg_rating.write.option("header", true).csv("file:///home/hadoop/nesrine/results/avg_ratings")
top_10_rated_movies.write.option("header",true).csv("file:///home/hadoop/nesrine/results/top_10_rated_movies")
number_ratings_user.write.option("header", true).csv("file:///home/hadoop/nesrine/results/number_ratings_user")
avg_rating_genre.write.option("header", true).csv("file:///home/hadoop/nesrine/results/avg_rating_genre")


spark.stop()
