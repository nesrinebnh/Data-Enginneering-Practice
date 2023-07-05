import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.DataTypes


// Create a SparkSession
val spark = SparkSession.builder().appName("CSV Reader").config("spark.sql.hive.convertMetastoreOrc", "true").enableHiveSupport().master("local").getOrCreate()

// Read CSV file into a DataFrame
val csvPath = "/user/hadoop/users.csv"  
val csvOptions = Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> ";")  

val df: DataFrame = spark.read.format("csv").options(csvOptions).load(csvPath)

// Show the DataFrame
df.show()


// Count number of null values in age column
val null_count = df.select(sum(col("Age").isNull.cast(DataTypes.LongType))).as[Long].head()
println(s"The number of null values in age column is $null_count")

val null_value_count = df.filter(col("Age") === "NULL").count()
println(s"The number of null values in age column is $null_value_count")

// Remove rows with null values
var clean_df = df.na.drop(Seq("Age"))
clean_df = clean_df.filter(col("Age")=!= "NULL")
clean_df.show()

// Cast Age column to int
clean_df = clean_df.withColumn("Age", col("Age").cast(DataTypes.IntegerType))


// Calculate a new column called "customer_category" based on the customer_age:
// If the customer_age is less than 18, categorize the customer as "Minor."
// If the customer_age is between 18 and 65, categorize the customer as "Adult."
// If the customer_age is greater than 65, categorize the customer as "Senior."

val altered_df:DataFrame = clean_df.withColumn("customer_category",when(col("Age") < 18, "Minor").when(col("Age")>=18 && col("Age")<=65, "Adult").otherwise("Senior")).withColumnRenamed("User-ID", "user_id").withColumnRenamed("Location", "customer_location").withColumnRenamed("Age", "customer_age")
altered_df.show()

// Save results in hive
spark.catalog.setCurrentDatabase("e_commerce")
altered_df.write.format("orc").mode("overwrite").partitionBy("customer_category").saveAsTable("users")


spark.stop()



