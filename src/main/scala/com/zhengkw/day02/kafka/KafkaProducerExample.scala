package com.zhengkw.day02.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}


/**
 * @ClassName:KafkaProducer
 * @author: zhengkw
 * @description:
 * @date: 20/06/09下午 4:59
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object KafkaProducerExample {
  def main(args: Array[String]): Unit = {
    writeToKafka("test")
  }

  def writeToKafka(topic: String) = {
    val props = new Properties()

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    val producer = new KafkaProducer[String, String](props)
    producer.send(new ProducerRecord[String, String](topic, "zhengkw"))
  }
}
