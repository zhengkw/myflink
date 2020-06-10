package com.zhengkw.day03

import com.zhengkw.day02.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @ClassName:RedisMapperExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/10上午 8:52
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object SinkToRedis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)
    // redis的主机
    val conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build()
    stream.addSink(new RedisSink[SensorReading](conf, new MyRedis))
    env.execute()
  }

  class MyRedis extends RedisMapper[SensorReading] {
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET, "sensor")
    }

    override def getKeyFromData(data: SensorReading): String = data.id

    override def getValueFromData(data: SensorReading): String = data.temperature.toString
  }

}
