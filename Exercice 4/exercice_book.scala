
// Instructions
// Write a Spark program in Scala to read a CSV file containing information about books (title, author, year, etc.). 
// Filter the books published after a specific year and transform the data to include only the title and author. 
// Print the resulting dataset.



import org.apache.spark.sql.{SparkSession, DataFrame}

// Create a SparkSession
val spark = SparkSession.builder().appName("CSV Reader").master("local[*]").getOrCreate()

// Read CSV file into a DataFrame
val csvPath = "/user/hadoop/books.csv"  
val csvOptions = Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> ";")  

val df: DataFrame = spark.read.format("csv").options(csvOptions).load(csvPath)

// Show the DataFrame
//df.show()

//filter the book that have been published after 2000 
val books_published_after_2000: DataFrame = df.filter(df("Year-Of-Publication") > 2000)
//books_published_after_2000.show()

//filter oly the title and author
val books_title_author: DataFrame = books_published_after_2000.select("Book-Title","Book-Author")
books_title_author.show()
