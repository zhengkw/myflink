package com.zhengkw.day07

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map

/**
 * @ClassName:CEPExample
 * @author: zhengkw
 * @description:
 * @date: 20/06/17上午 8:53
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object CEPExample {

  case class LoginEvent(userId: String, ip: String, eventType: String, eventTime: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .fromElements(
        LoginEvent("user_1", "0.0.0.0", "fail", 1000L),
        LoginEvent("user_1", "0.0.0.1", "fail", 2000L),
        LoginEvent("user_1", "0.0.0.2", "fail", 3000L),
        LoginEvent("user_2", "0.0.0.0", "success", 4000L)
      )
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.userId)
    // 定义需要匹配的模板
    val pattern = Pattern.begin[LoginEvent]("begin").where(_.eventType.equals("fail"))
      .next("second").where(_.eventType.equals("fail"))
      .next("other").where(_.eventType.equals("fail"))

      .within(Time.seconds(10))
    //.followedBy("end").where(_.eventType.equals("success"))

    val patternedStream = CEP.pattern(stream, pattern)
    patternedStream
      .select(func)
      .print
    env.execute()
  }

  val func = (pattern: Map[String, Iterable[LoginEvent]]) => {
    val begin = pattern.getOrElse("begin", null).iterator.next()
    val second = pattern.getOrElse("second", null).iterator.next()
    val other = pattern.getOrElse("other", null).iterator.next()
    //    val second = pattern.getOrElse("second", null).iterator.next()
    //    val third = pattern.getOrElse("third", null).iterator.next()


    s"${begin.userId} 连续三次登录失败！---- ${begin.ip}--${second.ip}--${other.ip}"


  }
}
