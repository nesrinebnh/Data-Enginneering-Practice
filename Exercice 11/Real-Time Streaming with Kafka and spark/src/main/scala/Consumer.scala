import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import scala.collection.JavaConverters._

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

    val consumer = new KafkaConsumer[String, String](props)

    // Subscribe to the topic
    consumer.subscribe(Seq(topic).asJava)

    // Continuously poll for new records
    while (true) {
      val records = consumer.poll(java.time.Duration.ofMillis(100))

      records.iterator().asScala.foreach { record =>
        val key = record.key()
        val value = record.value()
        val partition = record.partition()
        val offset = record.offset()

        // Process the received record
        println(s"Received record: key = $key, value = $value, partition = $partition, offset = $offset")
      }
    }
  }

}
