import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer,ProducerConfig,ProducerRecord}
import com.typesafe.config.ConfigFactory

object KafkaProducerExample {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load()
    // first arg prod(AWS) or dev(local)
    val envProps = conf.getConfig(args(0))
    // second arg filename to process
    val fileName = args(1)
    println(fileName)

    val lineIter = io.Source.fromFile(fileName).getLines()

    println("zookeeper: " + envProps.getString("zookeeper"))
    println("bootstrap-server: " + envProps.getString("bootstrap.server"))

    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getString( "bootstrap.server"))
    props.put(ProducerConfig.CLIENT_ID_CONFIG,"KafkaProducerExample")
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, envProps.getString( "batch.size"))
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")


    val producer = new KafkaProducer[String, String](props)

    while (lineIter.hasNext) {
      Thread.sleep(1)
      val msg = lineIter.next()
      val ipAddr = msg.split(",")(0)
      println(msg)
      val record = new ProducerRecord[String, String]("edgar-logs" , ipAddr, msg)
      producer.send(record)
    }


    producer.close()


  }

}
