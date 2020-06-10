package com.zhengkw.day03

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.zhengkw.day02.{SensorReading, SensorSource}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._

/**
 * @ClassName:SinkToMysql
 * @author: zhengkw
 * @description:
 * @date: 20/06/10上午 10:27
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object SinkToMySQL {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)
    stream.addSink(new MyJDBCSink)
    env.execute()
  }

  class MyJDBCSink extends RichSinkFunction[SensorReading] {
    // 连接
    var conn: Connection = _
    // 插入语句
    var insertStmt: PreparedStatement = _
    // 更新语句
    var updateStmt: PreparedStatement = _

    /**
     * @descrption: 只执行一次
     *              初始化mysql
     * @param parameters
     * @return: void
     * @date: 20/06/10 上午 10:37
     * @author: zhengkw
     */
    override def open(parameters: Configuration): Unit = {
      val conn = DriverManager.getConnection(
        "jdbc:mysql://hadoop102:3306/test",
        "root",
        "sa"
      )
      // 插入语句
      insertStmt = conn.prepareStatement(
        "INSERT INTO temperatures (sensor, temp) VALUES (?, ?)"
      )
      //更新语句
      updateStmt = conn.prepareStatement(
        "UPDATE temperatures SET temp = ? WHERE sensor = ?"
      )
    }

    override def invoke(value: SensorReading): Unit = {
      updateStmt.setDouble(1, value.temperature)
      updateStmt.setString(2, value.id)
      updateStmt.execute()

      if (updateStmt.getUpdateCount == 0) {
        insertStmt.setString(1, value.id)
        insertStmt.setDouble(2, value.temperature)
        insertStmt.execute()
      }
    }

    override def close(): Unit = {
      insertStmt.close()
      updateStmt.close()
      conn.close()
    }
  }

}


