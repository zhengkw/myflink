package com.zhengkw.project

import java.lang
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListStateDescriptor, ValueState}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @ClassName:HotItermsTopN
 * @author: zhengkw
 * @description:
 * @date: 20/06/19下午 4:34
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object HotItemsTopN {

  case class UserBehavior(userId: Long,
                          itemId: Long,
                          categoryId: Long,
                          behavior: String,
                          timestamp: Long)

  case class ItemViewCount(itemId: Long,
                           windowEnd: Long,
                           count: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.readTextFile("E:\\IdeaWorkspace\\myflink\\source\\UserBehavior.csv")
      .map(line => {

        val arr = line.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toLong, arr(3), arr(4).toLong * 1000L)
      })
      .filter(_.behavior.equals("pv"))
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      //聚合完 的结果是
      .aggregate(new CountAgg, new MyWindowFunction)
      .keyBy(_.windowEnd)
      .process(new TopN(3))

    stream.print()
    env.execute()
  }

  /**
   * @descrption: 实现累加器
   * @return:
   * @date: 20/06/19 下午 4:45
   * @author: zhengkw
   */
  class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
    /**
     * @descrption: 初始化累加器
     * @return: long
     * @date: 20/06/19 下午 4:45
     * @author: zhengkw
     */
    override def createAccumulator(): Long = 0L

    /** *
     *
     * @descrption: 累加逻辑
     * @param value
     * @param accumulator
     * @return: long
     * @date: 20/06/19 下午 4:45
     * @author: zhengkw
     */
    override def add(value: UserBehavior, accumulator: Long): Long = {
      accumulator + 1
    }

    /**
     * @descrption: 获取结果
     * @param accumulator
     * @return: long
     * @date: 20/06/19 下午 4:46
     * @author: zhengkw
     */
    override def getResult(accumulator: Long): Long = accumulator

    /**
     * @descrption: 合并累加器
     * @param a
     * @param b
     * @return: long
     * @date: 20/06/19 下午 4:46
     * @author: zhengkw
     */
    override def merge(a: Long, b: Long): Long = a + b
  }

  /**
   * @descrption: 全窗口函数为了得到窗口结束时间！
   * @return: 商品id  窗口结束时间  累加器运算的结果
   * @date: 20/06/19 下午 5:02
   * @author: zhengkw
   */
  class MyWindowFunction extends ProcessWindowFunction[Long, ItemViewCount, Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      out.collect(ItemViewCount(key, context.window.getEnd, elements.head))
    }
  }

  /**
   * @descrption: 排序
   * @return:
   * @date: 20/06/19 下午 5:03
   * @author: zhengkw
   */
  class TopN(val topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
    //存放商品的id+ window end+ count
    lazy val listState = getRuntimeContext.getListState(
      new ListStateDescriptor[ItemViewCount]("list-state", classOf[ItemViewCount])
    )

    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {

      listState.add(value) //为什么不是先判断状态变量里有没有该id的数据 然后更新或者直接add
      // 不会重复注册 ？？
      //为什么多+100ms触发，为了保证数据都到了吗？？
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
    }


    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      val allItems: ListBuffer[ItemViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      // 将列表状态中的数据转移到内存
      for (item <- listState.get) {
        allItems += item
      }
      // 清空状态
      listState.clear()

      // 使用浏览量降序排列
      val sortedItems = allItems.sortBy(-_.count).take(topSize)

      val result = new StringBuilder

      result
        .append("==========================\n")
        .append("时间：")
        .append(new Timestamp(timestamp - 100))
        .append("\n")

      for (i <- sortedItems.indices) {
        val currItem = sortedItems(i)
        result
          .append("No.")
          .append(i + 1)
          .append(":")
          .append("  商品ID = ")
          .append(currItem.itemId)
          .append("  浏览量 = ")
          .append(currItem.count)
          .append("\n")
      }
      result
        .append("===========================\n\n\n")
      Thread.sleep(1000)
      out.collect(result.toString)
    }
  }

}
