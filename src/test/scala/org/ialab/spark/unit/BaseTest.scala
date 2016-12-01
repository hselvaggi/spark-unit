package org.ialab.spark.unit

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

import org.ialab.spark.unit.StreamingUnit._
/**
  * Created by hselvaggi on 20/11/16.
  */
class BaseTest extends StreamingUnit[Double]("/Users/hselvaggi/checkpointing", Seconds(5), Seconds(5)) {

  sparkTest("Simple test", 12000) {
    implicit val transformed: DStream[Double] = dstream.map(x => (1, x)).reduce((x, y) => (x._1 + y._1, x._2 + y._2)).map(x => x._2 / x._1)

    publish(0, Seq[Double](1, 2))
    publish(1, Seq[Double](2, 2))

    validateStep[Double](transformed, 0, (rdd: RDD[Double]) => assert(rdd.collect()(0) == 1.5))
    validateStep[Double](1, (rdd: RDD[Double]) => assert(rdd.collect()(0) == 2.0))
  }

}
