package com.zhengkw.day01

import org.apache.flink.streaming.api.scala._
/**
 * @ClassName:WordCountFromBatch
 * @author: zhengkw
 * @description:
 * @date: 20/06/08下午 12:45
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
object WordCountFromBatch {
  def main(args: Array[String]): Unit = {
    // 获取运行时环境，类似SparkContext
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 并行任务的数量设置为1
    env.setParallelism(1)

    val stream = env
      .fromElements(
        "zuoyuan",
        "hello world",
        "zuoyuan",
        "zuoyuan"
      )

    // 对数据流进行转换算子操作
    val textStream = stream
      // 使用空格来进行切割输入流中的字符串
      .flatMap(r => r.split("\\s"))
      // 做map操作, w => (w, 1)
      .map(w => WordWithCount(w, 1))
      // 使用word字段进行分组操作，也就是shuffle
      .keyBy(0)
      // 做聚合操作，类似与reduce
      .sum(1)

    // 将数据流输出到标准输出，也就是打印
    textStream.print()

    // 不要忘记执行！
    env.execute()
  }

  case class WordWithCount(word: String, count: Int)

}
