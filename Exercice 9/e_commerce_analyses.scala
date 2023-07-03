import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.{TimestampType, DataTypes}
import java.time.Duration

// Create a SparkSession
val spark = SparkSession.builder().appName("e-commerce").master("local").getOrCreate()

// Read CSV file into a DataFrame
val csvPath = "/user/hadoop/olist_orders_dataset.csv"
val csvOptions = Map("header" -> "true", "inferSchema" -> "true")

val orders: DataFrame = spark.read.format("csv").options(csvOptions).load(csvPath)

// Show the DataFrame
orders.show()


val custPath = "/user/hadoop/olist_customers_dataset.csv"
val custOptions = Map("header" -> "true", "inferSchema" -> "true")

val customers: DataFrame = spark.read.format("csv").options(custOptions).load(custPath)

// Show the DataFrame
customers.show()


// Calculate the total number of orders
val number_orders = orders.count()
println(s"the number of orders is $number_orders")

// Count the unique customers
val number_unique_customers = customers.select("customer_unique_id").distinct().count()
println(s"the number of customers is $number_unique_customers")

// Determine the order status distribution
val order_distribution = orders.groupBy("order_status").agg(count(col("order_id")).alias("count orders"))
println("the order status distribution")
order_distribution.show()

// Calculate the average time between order placement and order approval
val convert_date_timestamp = orders.withColumn("order_purchase", to_timestamp($"order_purchase_timestamp")).withColumn("order_approved_at_timestamp", to_timestamp($"order_approved_at"))
val time_diff = convert_date_timestamp.withColumn("time_diff(seconds)", unix_timestamp($"order_approved_at_timestamp") - unix_timestamp($"order_purchase"))
println("the average time between order placement and order approval")
time_diff.select("order_id", "order_approved_at_timestamp", "order_purchase", "time_diff(seconds)").show()


// Join the orders and customers DataFrames based on the customer_id
val df: DataFrame = orders.join(customers, Seq("customer_id"), "inner")
df.show()

// Find the top 10 cities with the highest number of orders
val top_10_cities: DataFrame = df.groupBy("customer_city").agg(count(col("order_id")).alias("count_orders")).orderBy(col("count_orders").desc).limit(10)
println("the top 10 cities with the highest number of orders")
top_10_cities.show()

// Calculate the average delivery time for orders
val avg_delivery_time = time_diff.groupBy("order_id").agg(avg(col("time_diff(seconds)")).cast(DataTypes.LongType).alias("avg_delivery_time")).select("avg_delivery_time").as[Long].head()

val duration = Duration.ofSeconds(avg_delivery_time)
val days = duration.toDays
val hours = duration.toHours % 24
val minutes = duration.toMinutes % 60
val seconds = duration.getSeconds % 60

val averageTimeReadable = s"$days days, $hours hours, $minutes minutes, $seconds seconds"

println("Average time between order placement and approval: " + averageTimeReadable)

// Determine the customer state with the most orders
val customer_state_most_orders = df.groupBy("customer_state").agg(count(col("order_id")).alias("order_count")).orderBy(col("order_count").desc).limit(1)
println(s"the customer state with the most orders")
customer_state_most_orders.show()



spark.stop()
