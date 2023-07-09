import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.collection.JavaConverters._
import java.sql.{Connection, DriverManager, SQLException}
import io.circe._
import io.circe.parser._
import org.postgresql.util.PSQLException


object Consumer {
  def main(args: Array[String]): Unit = {
    val topic = "tv-shows"

    // Kafka consumer configuration
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092") // Kafka bootstrap servers
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "tv-shows")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") // Start consuming from the beginning of the topic



    // Connect to postgres
    val url = "jdbc:postgresql://IPADDRESS:PORT/DATABASE"
    val username = "USERNAME"
    val password = "PASSWORD"

    val connection: Connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement()


    val consumer = new KafkaConsumer[String, String](props)

    // Subscribe to the topic
    consumer.subscribe(Seq(topic).asJava)

    // Continuously poll for new records
    while (true) {
      val records = consumer.poll(java.time.Duration.ofMillis(100))

      records.iterator().asScala.foreach { record =>

        val value = record.value()
        val json: Either[ParsingFailure, Json] = parse(value)
        json match {
          case Left(parseFailure) => println(s"Failed to parse JSON: $parseFailure")
          case Right(jsonData) =>
            val person: Either[Error, TvShows] = jsonData.as[TvShows]


            person match {
              case Left(decodingFailure) => println(s"Failed to decode JSON to TvShows: $decodingFailure")
              case Right(shows) => {
                println(s"Successfully parsed TvShows: $shows")
                val title: String = shows.title.replace("'", " ")
                val sql = s"INSERT INTO tvshows_stg (rowID,id,title,year,age,IMDb,RottenTomatoes,isNetflix,isHulu,isPrimeVideo) VALUES ('${shows.rowID}','${shows.id}','$title','${shows.year}','${shows.age}','${shows.IMDb}','${shows.RottenTomatoes}','${shows.isNetflix}','${shows.isHulu}','${shows.isPrimeVideo}')"
                try {
                  statement.executeUpdate(sql)
                  println("Data inserted successfully!")
                } catch {
                  case e: PSQLException if e.getSQLState == "23505" =>
                    println("Skipping duplicate key violation error.")
                  case e: SQLException =>
                    println("An unexpected SQL error occurred: " + e.getMessage)
                }

              }
            }
        }
      }

    }
    statement.close()
    connection.close()

  }

}
