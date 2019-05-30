## API-4-键值转换操作：partitionBy，mapValues，flatMapValues，combineByKey，foldByKey，reduceByKey，groupByKey，
sortByKey，cogroup，subtractByKey，join，leftOuterJoin，rightOuterJoin，fullOuterJoin

### 1.partitionBy(只能用于 PairRdd)：
* 源码API:
```scala
def partitionBy(partitioner : org.apache.spark.Partitioner) : org.apache.spark.api.java.JavaPairRDD[K, V] = { /* compiled code */ }
```
partitionBy函数对RDD进行分区操作。
函数定义如下。
　　partitionBy（partitioner：Partitioner）
　　如果原有RDD的分区器和现有分区器（partitioner）一致，则不重分区，如果不一致，则相当于根据分区器生成一个新的ShuffledRDD。
