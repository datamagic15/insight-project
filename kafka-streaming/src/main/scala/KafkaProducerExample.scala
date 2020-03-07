import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer,ProducerConfig,ProducerRecord}

object KafkaProducerExample {
  def main(args: Array[String]): Unit = {
    // second arg filename to process
    val fileName = args(0)
    println(fileName)
    val bootstrap_server = sys.env("BOOTSTRAP_SERVER")
    val batch_size = "16384"
    val kafka_topic = "edgar-logs"

    val lineIter = io.Source.fromFile(fileName).getLines()

    println("bootstrap-server: " + bootstrap_server)

    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server)
    props.put(ProducerConfig.CLIENT_ID_CONFIG,"KafkaProducerExample")
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, batch_size)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")


    val producer = new KafkaProducer[String, String](props)

    while (lineIter.hasNext) {
      val msg = lineIter.next()
      val ipAddr = msg.split(",")(0)
      val record = new ProducerRecord[String, String](kafka_topic, ipAddr, msg)
      producer.send(record)
    }


    producer.close()


  }

}
