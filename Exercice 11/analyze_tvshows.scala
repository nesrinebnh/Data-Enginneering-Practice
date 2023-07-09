import org.apache.spark.sql.{SaveMode,SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.util.Properties

// Get Spark Session
val spark = SparkSession.builder().appName("TvShows Analyses").master("local").getOrCreate()

// Postgres configuration settings
val connectionProperties = new Properties()
connectionProperties.put("user", "edbuser")
connectionProperties.put("password", "edbuser")
connectionProperties.put("driver", "org.postgresql.Driver")

val jdbcUrl = "jdbc:postgresql://10.10.70.46:5433/edbstore"
val tableName = "tvshows"

// Retrieve data from postgres table
var df: DataFrame = spark.read.jdbc(jdbcUrl, tableName, connectionProperties)
df.show()

val count_tv_shows_per_year: DataFrame = df.groupBy("year").agg(count("title").alias("count tv shows"))
count_tv_shows_per_year.show()

val top_tv_shows: DataFrame = df.orderBy(col("imdb").desc).select("title","imdb", "year", "age")
top_tv_shows.show()

val netflix_count: DataFrame = df.groupBy("isnetflix").agg(count("title").alias("netflix_count"))
netflix_count.show()

val top_tv_show_netflix: DataFrame = df.orderBy(col("imdb").desc).filter(col("isnetflix") === true).select("title", "imdb", "year")
top_tv_show_netflix.show()

val top_tv_shows_prime: DataFrame = df.orderBy(col("imdb").desc).filter(col("isprimevideo") === true).select("title", "imdb", "year")
top_tv_shows_prime.show()

val top_tv_shows_hulu: DataFrame = df.orderBy(col("imdb").desc).filter(col("ishulu") === true).select("title", "imdb", "year")
top_tv_shows_hulu.show()

val top_tv_shows_all_screens: DataFrame = df.orderBy(col("imdb").desc).filter(col("ishulu") === true && col("isprimevideo") === true && col("isnetflix") === true).select("title", "imdb", "year")
top_tv_shows_all_screens.show()

spark.stop()