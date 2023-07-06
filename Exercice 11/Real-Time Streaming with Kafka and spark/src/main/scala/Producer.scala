
import java.io.File
import java.util.{Properties, Timer, TimerTask}
import scala.io.Source
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import play.api.libs.json._

object Producer {
  def main(args: Array[String]): Unit = {
    println("Hello world!")
    val csvFile = new File("src/main/scala/tv_shows.csv")
    val topic = "tv-shows"

    // Kafka producer configuration
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092") // Kafka bootstrap servers
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    try {
      // Read the CSV file
      var lines = Source.fromFile(csvFile).getLines().toList

      // Drop the header
      val updatedLines = lines.drop(1) // Remove the sent line from the list
      lines = updatedLines

      while (lines.nonEmpty) {
        val line = lines.head

        // Extract attributes
        val attributes = line.split(",")
        val rowID = attributes(0)
        val id = attributes(1)
        val title = attributes(2)
        val year = attributes(3)
        val age = attributes(4)
        val IMDb = attributes(5)
        val RottenTomatoes = attributes(6)
        val isNetflix = attributes(7)
        val isHulu = attributes(8)
        val isPrimeVideo = attributes(9)

        val json: JsObject = Json.obj(
          "rowID" -> rowID,
          "id" -> id,
          "title" -> title,
          "year" -> year,
          "age" -> age,
          "IMDb" -> IMDb,
          "RottenTomatoes" -> RottenTomatoes,
          "isNetflix" -> isNetflix,
          "isHulu" -> isHulu,
          "isPrimeVideo" -> isPrimeVideo
        )

        val jsonString = Json.stringify(json)

        // Send one line from the file
        val record = new ProducerRecord[String, String](topic, jsonString)
        println(record)
        producer.send(record)

        // Remove the sent line from the list
        val updatedLines = lines.drop(1)
        lines = updatedLines

        Thread.sleep(60000) // Sleep for 1 minute
      }

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      producer.close()
    }




    producer.close()
  }
}