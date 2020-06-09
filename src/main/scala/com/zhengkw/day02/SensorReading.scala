package com.zhengkw.day02

/**
 * @ClassName:SensorReading
 * @author: zhengkw
 * @description: 模拟温度传感器读数
 *              id  传感器id
 *              ts 时间戳
 *              temper 温度
 * @date: 20/06/09上午 9:28
 * @version:1.0
 * @since: jdk 1.8 scala 2.11.8
 */
case class SensorReading(id: String,
                         timestamp: Long,
                         temperature: Double)
