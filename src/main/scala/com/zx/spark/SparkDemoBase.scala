package com.zx.spark

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.{SparkConf, SparkContext}

abstract class SparkDemoBase extends Logging {

  def stat(): Unit

  def main(args: Array[String]): Unit = {
    init()
    stat()
    close()
  }

  def init(): SparkContext = {
    logger.info("spark 初始化。。。")
    val conf = new SparkConf()
      .setAppName("SparkDemo")
      .setMaster("local[*]")
    SparkDemoBase.sc = new SparkContext(conf)
    SparkDemoBase.sc.setLogLevel("warn")
    logger.info("spark 初始化完成！")
    SparkDemoBase.sc
  }

  def close(): Unit = {
    logger.info("关闭SparkContext")
    SparkDemoBase.sc.stop()
  }
}

object SparkDemoBase {
  var sc: SparkContext = _
}
