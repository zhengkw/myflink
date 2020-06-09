package com.zhengkw.day02.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
 * @ClassName:KafkaConsumerExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/09下午 5:19
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object KafkaConsumerExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val props = new Properties()
    props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    props.put("group.id", "consumer-group")
    props.put(
      "key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserialization"
    )
    props.put(
      "value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserialization"
    )
    props.put("auto.offset.reset", "latest")

    val stream = env
      .addSource(
        new FlinkKafkaConsumer011[String](
          "test",
          new SimpleStringSchema(),
          props
        )
      )

    /*  stream.addSink(
        new FlinkKafkaProducer011[String](
          "hadoop102:9092,hadoop103:9092,hadoop104:9092",
          "test",
          new SimpleStringSchema()
        )
      )*/

    stream.print()
    env.execute()

  }
}
