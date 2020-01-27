import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer,ProducerConfig,ProducerRecord}
import com.typesafe.config.ConfigFactory

object KafkaProducerExample {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load()
    // first arg prod or dev
    val envProps = conf.getConfig(args(0))
    // second arg file to process
    val fileName = args(1)

    val lineIter = io.Source.fromFile(fileName).getLines()

    println("zookeeper: " + envProps.getString("zookeeper"))
    println("bootstrap-server: " + envProps.getString("bootstrap.server"))

    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getString( "bootstrap.server"))
    props.put(ProducerConfig.CLIENT_ID_CONFIG,"KafkaProducerExample")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")


    val producer = new KafkaProducer[String, String](props)
    while (lineIter.hasNext) {
      val msg = lineIter.next()
      val record = new ProducerRecord[String, String]("Kafka-Testing","country", msg)
      producer.send(record)
    }

//    val record1 = new ProducerRecord[String, String]("Kafka-Testing","country","Roger Creager")
//    producer.send(record1)

    producer.close()


  }

}
