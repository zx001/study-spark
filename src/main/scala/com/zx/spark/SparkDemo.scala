package com.zx.spark

object SparkDemo extends SparkDemoBase {
  override def stat(): Unit = {
    val data: Array[Int] = Array(1, 2, 3, 4, 5)
    val sd: Seq[Int] = data.toSeq
    val distData = SparkDemoBase.sc.parallelize(sd, 1)
    distData.collect().foreach(println)
  }
}
