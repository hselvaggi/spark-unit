Spark-Unit
==========

Spark-unit is a pretty simple unit testing framework that intends to help unit test spark streaming applications. Creating spark streaming applications can be challenging as it is hard to validate that our code does what we want. As our code applies different transformations to lots of data being able to properly verify the final results of all the transformations
is hard as we may not know the expected results.


Creating unit tests for spark streaming has some complexities but it pays of by helping us to ensure that our code is correct. In one hand streaming data comes in small batches and to properly test this we need to be able to simulate multiple batches in a test. On the other hand as data comes into different batches we may want to validate that the intermediates results after each batch are right.

What can be tested
==================

The only thing that makes sense to be able to test are the transformations applied to the data that is comming into the DStream in complete isolation from the external world. This means without connections to any data source like Cassandra, Kafka or others as well as no place to store the data.

Making this restriction is also good as it will help to properly structure the source code by spliting concerns. One function or even class will be responsible of transforming the data returning a DStream with all the transformation applied to which will make sense to apply assertions. Creating this DStream as well as storing the results of the computations is something that must be splited into a different method or even class. This is needed to be able to tests the code and at the same time forces the developer to apply good practices.
   
*Future versions of this document will propose a code structure for streaming applications that may be good from the software engineering point of view as well as a good structure that will help to properly unit test the application*

Writting unit tests for spark streaming
=======================================

To unit tests our Spark Streaming application our test class must extend StreamingUnit class. This class is parametrized by the type that is held by the DStream that is the input to our pipeline. As streaming application may need checkpoiting to be enabled it is constructed with three parameters: The path for storing checkpoint information, followed by the window time and the window slide timebox.

As an example the following code is taken from BaseTest class included in the tests of this project.

```scala
class BaseTest extends StreamingUnit[Double]("/Users/hselvaggi/checkpointing", Seconds(5), Seconds(5))
```

Declaring a test method is done as follows

```scala
sparkTest("Simple test", 12000) {
   // Here comes the test code
}
```

Publishing data into a specific step is done by calling the publish method as in the following example.

```scala
publish(0, Seq[Double](1, 2))
```

Here 0 represent the time when the data is available on the DStream which represent the batch time when that data will get processed. The Seq object must be parametrized by the same type of the input DStream which is the type parameter of the StreamingUnit class. All the data in that Seq object will be made available to the DStream at the specified time.

Validating the result of processing a specific time batch can be done as follows:

```scala
validateStep[Double](1, (rdd: RDD[Double]) => assert(rdd.collect()(0) == 2.0))
```

Here again 1 represent the time batch (1 is the second batch being processed) and the second parameter is a validation function that will hold all the assertions on the final results.
