package com.zhengkw.day03

import java.util

import com.zhengkw.day02.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

/**
 * @ClassName:SinkToES
 * @author: zhengkw
 * @description:
 * @date: 20/06/10上午 9:36
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object SinkToES {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)

    // es的主机和端口
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop102", 9200))

    // 定义了如何将数据写入到es中去
    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHosts, // es的主机名
      // 匿名类，定义如何将数据写入到es中
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading,
                             runtimeContext: RuntimeContext,
                             requestIndexer: RequestIndexer): Unit = {
          // 哈希表的key为string，value为string
          val json = new util.HashMap[String, String]()
          json.put("data", t.toString)
          // 构建一个写入es的请求
          val indexRequest = Requests
            .indexRequest()
            .index("sensor") // 索引的名字是sensor
            .`type`("zhengkw")
            //传入的是一个hash表
            .source(json)

          requestIndexer.add(indexRequest)
        }
      }
    )

    // 用来定义每次写入多少条数据
    // 成批的写入到es中去
    esSinkBuilder.setBulkFlushMaxActions(10)

    stream.addSink(esSinkBuilder.build())

    env.execute()

  }
}
