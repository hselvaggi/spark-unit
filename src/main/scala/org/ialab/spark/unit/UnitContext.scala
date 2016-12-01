package org.ialab.spark.unit

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable

/**
  * Created by hselvaggi on 26/11/16.
  */
class UnitContext[A](checkpointPath: String, window: Duration, slide: Duration) {
  val conf = new SparkConf().setAppName("StreamingUnit").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val ssc = new StreamingContext(sc, window)

  val session = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  sc.setCheckpointDir(checkpointPath)
}