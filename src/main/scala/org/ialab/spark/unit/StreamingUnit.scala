package org.ialab.spark.unit

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.{Assertion, FunSuite}

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * Created by hselvaggi on 19/11/16.
  */
class StreamingUnit[A: ClassTag](checkpointPath: String, window: Duration, slide: Duration) extends FunSuite {
  private var dstreamVar: DStream[A] = null
  private var rddVar: mutable.Queue[RDD[A]] = null
  private var context: UnitContext[A] = null

  def sc: SparkContext = context.sc

  def ssc: StreamingContext = context.ssc

  def session: SparkSession = context.session

  def dstream: DStream[A] = dstreamVar

  def rdd: mutable.MutableList[RDD[A]] = rddVar

  def validateStep[B](stream: DStream[B], step: Int, fn: RDD[B] => Unit): Unit = validateStep(step, fn)(stream)

  def validateStep[B](step: Int, fn: RDD[B] => Unit)(implicit stream: DStream[B]): Unit = {
    var counter = 0

    stream.foreachRDD(r => {
      if (counter == step) fn(r)
      counter = counter + 1
    })
  }

  def publish(step: Int, fn: => Seq[A]) = {
    var counter = 1

    if (step == 0) {
      val newData: RDD[A] = sc.makeRDD(fn)
      rdd += sc.makeRDD[A](fn)
    } else {
      dstream.foreachRDD(r => {
        if (counter == step) {
          rdd += sc.makeRDD(fn)
        }
        counter = counter + 1
      })
    }
  }

  def sparkTest(desc: String, duration: Long)(testFun: => Unit) = {
    context = new UnitContext[A](checkpointPath, window, slide)
    rddVar = mutable.Queue[RDD[A]]()

    dstreamVar = ssc.queueStream(rddVar)
    def executeMe = {
      testFun
      ssc.start()
      ssc.awaitTerminationOrTimeout(duration)
    }

    test(desc)(executeMe)
  }

}

object StreamingUnit {
  implicit def validationToUnit[A](fn: RDD[A] => Assertion) : (RDD[A] => Unit) = {
    def convertedFunction(rdd: RDD[A]): Unit = { fn(rdd) }
    convertedFunction
  }
}
