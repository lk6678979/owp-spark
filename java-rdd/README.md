# SPARK的JAVA版API案例
## 1.创建RDD
### 1.1使用已存在的集合创建RDD（自己创建数据生成RDD）
####  parallelize（使用集合创建RDD）
##### 源码API
```scala
//根据List创建RDD，numSlices：分区数
def parallelize[T](list : java.util.List[T], numSlices : scala.Int) : org.apache.spark.api.java.JavaRDD[T] = { /* compiled code */ }
//根据List创建RDD，自动分区
def parallelize[T](list : java.util.List[T]) : org.apache.spark.api.java.JavaRDD[T] = { /* compiled code */ }
//根据List创建Pairs的RDD，第一个对象是scala.Tuple2对象集合，numSlices：分区数
def parallelizePairs[K, V](list : java.util.List[scala.Tuple2[K, V]], numSlices : scala.Int) : org.apache.spark.api.java.JavaPairRDD[K, V] = { /* compiled code */ }
//根据List创建Pairs的RDD，第一个对象是scala.Tuple2对象集合
def parallelizePairs[K, V](list : java.util.List[scala.Tuple2[K, V]]) : org.apache.spark.api.java.JavaPairRDD[K, V] = { /* compiled code */ }
//根据List<Double>创建Double数据类型的RDD，numSlices：分区数
def parallelizeDoubles(list : java.util.List[java.lang.Double], numSlices : scala.Int) : org.apache.spark.api.java.JavaDoubleRDD = { /* compiled code */ }
//根据List<Double>创建Double数据类型的RDD
def parallelizeDoubles(list : java.util.List[java.lang.Double]) : org.apache.spark.api.java.JavaDoubleRDD = { /* compiled code */ }
```
##### JAVA API
[1. JAVA API:](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/Parallelize.java)   
