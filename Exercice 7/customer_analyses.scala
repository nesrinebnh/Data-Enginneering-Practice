import org.apache.spark.sql.{SparkSession, DataFrame, SaveMode}

// Create a SparkSession
val spark = SparkSession.builder().appName("CSV Reader").master("local").getOrCreate()

// Read CSV file into a DataFrame
val csvPath = "/user/hadoop/restaurant-1-orders.csv"  
val csvOptions = Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> ",")  

val df: DataFrame = spark.read.format("csv").options(csvOptions).load(csvPath)

// Show the DataFrame
df.show()

// Register the DataFrame as a temporary table in Hive
df.createOrReplaceTempView("nesrine.restaurant_orders_temp")

// set the database name 
spark.sql("use nesrine;")

// Perform basic data exploration using Spark SQL
// Count the total number of orders
val total_orders: DataFrame = spark.sql("select count(*) as number_orders from restaurant_orders_temp;")
println("The total number of orders is: ")
total_orders.show()


// Calculate the average order amount.
val avg_orders: DataFrame = spark.sql("select `Order Number`, round(sum(Quantity * `Product Price`),2) as avg_order_amount from restaurant_orders_temp group by `Order Number`;")
println("The average order amount is: ")
avg_orders.show()

// Find the minimum and maximum order amounts
var min_max_order: DataFrame = spark.sql("select `Order Number`, round(min(Quantity * `Product Price`),2) as min_order_amount,  round(max(Quantity * `Product Price`),2) as max_order_amount from restaurant_orders_temp group by `Order Number`;")
println("the minimum and maximum order amounts is: ")
min_max_order.show()

// Save the result of the analysis as a new Hive table
val order_analyses_sql: DataFrame = avg_orders.join(min_max_order, Seq("Order Number"), "inner").withColumnRenamed("Order Number", "order_number")
order_analyses_sql.show()

order_analyses_sql.write.mode(SaveMode.Overwrite).saveAsTable("restaurant_order_analyses")


spark.stop()

