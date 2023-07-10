import org.apache.spark.sql.{SaveMode,SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.util.Properties

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello world!")

    // Get Spark Session
    val spark = SparkSession.builder().appName("TvShows Analyses").master("local").getOrCreate()

    // Postgres configuration settings
    val connectionProperties = new Properties()
    connectionProperties.put("user", "edbuser")
    connectionProperties.put("password", "edbuser")
    connectionProperties.put("driver", "org.postgresql.Driver")

    val jdbcUrl = "jdbc:postgresql://10.10.70.46:5433/edbstore"
    val tableName = "tvshows_stg"

    // Retrieve data from postgres table
    var df: DataFrame = spark.read.jdbc(jdbcUrl, tableName, connectionProperties)
    df.show()

    // STEP1: VALIDATING AND CLEANING DATA

    // Extract the value of imdb and rottentomatoes columns
    df = df.withColumn("imdb", regexp_replace(col("imdb"), "/10", "").cast("double")).withColumn("rottentomatoes", regexp_replace(col("rottentomatoes"), "/100", "").cast("double"))
    df.show()

    // Handling missing values
    val nullCounts = df.select(df.columns.map(c => sum(col(c).isNull.cast("integer")).alias(c)): _*)
    nullCounts.show()

    // Only imdb column that contain null values. I will handle null values in this column only
    df = df.na.drop(Seq("imdb"))
    df.show()

    // Validate the type of each column
    val expectedSchema = Seq(
      ("rowid", "integer"),
      ("id", "integer"),
      ("title", "string"),
      ("year", "integer"),
      ("age", "string"),
      ("imdb", "double"),
      ("rottentomatoes", "integer"),
      ("isnetflix", "boolean"),
      ("ishulu", "boolean"),
      ("isprimevideo", "boolean")
    )

    val actualSchema = df.schema.map(field => (field.name, field.dataType.typeName))

    val mismatchedColumns = expectedSchema.filter { case (columnName, dataType) =>
      actualSchema.find { case (actualColumnName, actualDataType) =>
        actualColumnName == columnName && actualDataType != dataType
      }.isDefined
    }

    if (mismatchedColumns.nonEmpty) {
      println("Mismatched columns:")
      mismatchedColumns.foreach { case (columnName, dataType) =>
        val actualDataType = actualSchema.find(_._1 == columnName).map(_._2).getOrElse("Unknown")
        println(s"Column: $columnName, Expected: $dataType, Actual: $actualDataType")
      }
    } else {
      println("All columns have correct data types.")
    }


    // correct the type of mismatched columns
    val cleanDF = mismatchedColumns.foldLeft(df) { (tempDF, column) =>
      val columnName = column._1
      val expectedDataType = column._2

      // Cast the column to the expected data type
      val correctedColumn = col(columnName).cast(expectedDataType)

      // Replace the column in the DataFrame with the corrected column
      tempDF.withColumn(columnName, correctedColumn)
    }


    cleanDF.printSchema()
    cleanDF.show()

    // STEP2 SAVE THE CLEAN DF IN A NEW POSTGRES TABLE
    cleanDF.write.format("jdbc").mode(SaveMode.Overwrite).option("url", jdbcUrl).option("dbtable", "tvshows").option("driver", "org.postgresql.Driver").option("truncate", "true").option("batchsize", "1000").option("numPartitions", 5279).option("partitionColumn", "id").option("lowerBound", "1").option("upperBound", "6000").jdbc(jdbcUrl, "tvshows", connectionProperties)


    spark.stop()

  }
}