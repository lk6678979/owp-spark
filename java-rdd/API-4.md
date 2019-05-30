## API-4-键值转换操作：partitionBy，mapValues，flatMapValues，combineByKey，foldByKey，reduceByKey，groupByKey，sortByKey，cogroup，subtractByKey，join，leftOuterJoin，rightOuterJoin，fullOuterJoin

### 1.partitionBy(只能用于 PairRdd)：
* 源码API:
```scala
def partitionBy(partitioner : org.apache.spark.Partitioner) : org.apache.spark.api.java.JavaPairRDD[K, V] = { /* compiled code */ }
```
* partitionBy函数对RDD进行分区操作。
* 如果原有RDD的分区器和现有分区器（partitioner）一致，则不重分区，如果不一致，则相当于根据分区器生成一个新的ShuffledRDD。
### 2.mapValues：
* 源码API:
```scala
def mapValues[U](f : org.apache.spark.api.java.function.Function[V, U]) : org.apache.spark.api.java.JavaPairRDD[K, U] = { /* compiled code */ }
```
mapValues针对PairRDD的[K,V]中的V操作，返回一个新的PairRDD，key是原RDD的key，value是mapValues函数中返回的值
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/MapValues.java) 
```java
  @Test
    public void mapValues() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9"), 3);
        JavaPairRDD<String, String> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, String>(s, s));
        JavaPairRDD<String, String> RDD3 = RDD2.mapValues(e -> e + "1");
        System.out.println("mapValues后：" + RDD3.collect());
    }
------------------------返回------------------------------
[(1,11), (2,21), (3,31), (4,41), (5,51), (6,61), (7,71), (8,81), (9,91)]
```
### 3.flatMapValues：
* 源码API:
```scala
def flatMapValues[U](f : org.apache.spark.api.java.function.Function[V, java.lang.Iterable[U]]) : org.apache.spark.api.java.JavaPairRDD[K, U] = { /* compiled code */ }
```
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/FlatMapValues.java) 
```java
SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9"), 3);
JavaPairRDD<String, String> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, String>(s, s));
JavaPairRDD<String, String> RDD3 = RDD2.flatMapValues(s -> Arrays.asList(s, s + "1", s + 2));
System.out.println("mapValues后：" + RDD3.collect());
--------------------------------返回结果--------------------------
[(1,1), (1,11), (1,12), (2,2), (2,21), (2,22), (3,3), (3,31), (3,32)
, (4,4), (4,41), (4,42), (5,5), (5,51), (5,52), (6,6), (6,61), (6,62)
, (7,7), (7,71), (7,72), (8,8), (8,81), (8,82), (9,9), (9,91), (9,92)]
```
* 返回的PairRDD的key是原RDD的key，一个key会多次出现，value是value是函数中生成的值
### 4. combineByKey：
* 源码API:
```scala
def combineByKey[C](createCombiner : org.apache.spark.api.java.function.Function[V, C], mergeValue : org.apache.spark.api.java.function.Function2[C, V, C], mergeCombiners : org.apache.spark.api.java.function.Function2[C, C, C]) : org.apache.spark.api.java.JavaPairRDD[K, C] = { /* compiled code */ }
def combineByKey[C](createCombiner : org.apache.spark.api.java.function.Function[V, C], mergeValue : org.apache.spark.api.java.function.Function2[C, V, C], mergeCombiners : org.apache.spark.api.java.function.Function2[C, C, C], partitioner : org.apache.spark.Partitioner, mapSideCombine : scala.Boolean, serializer : org.apache.spark.serializer.Serializer) : org.apache.spark.api.java.JavaPairRDD[K, C] = { /* compiled code */ }
def combineByKey[C](createCombiner : org.apache.spark.api.java.function.Function[V, C], mergeValue : org.apache.spark.api.java.function.Function2[C, V, C], mergeCombiners : org.apache.spark.api.java.function.Function2[C, C, C], partitioner : org.apache.spark.Partitioner) : org.apache.spark.api.java.JavaPairRDD[K, C] = { /* compiled code */ }
def combineByKey[C](createCombiner : org.apache.spark.api.java.function.Function[V, C], mergeValue : org.apache.spark.api.java.function.Function2[C, V, C], mergeCombiners : org.apache.spark.api.java.function.Function2[C, C, C], numPartitions : scala.Int) : org.apache.spark.api.java.JavaPairRDD[K, C] = { /* compiled code */ }
```
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/MapValues.java) 
```java
  JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
JavaPairRDD<String, String> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, String>(s, s+"1"));
JavaPairRDD<String, String> RDD3 = RDD2.combineByKey(s->s+"v",(s1,s2)->s1+"合并"+s2,(s1,s2)->s1+"分区"+s2);
System.out.println("combineByKey：" + RDD3.collect());
------------------结果---------------
[(6,61v分区61v), (3,31v), (4,41v合并41), (7,71v), (1,11v), (8,81v), (2,21v)]
```
* 需要传入三个自定义的方法，现在重点说说这三个方法的意义：
* 首先紧跟着Aggregator的三个泛型，第一个K，这个是你进行combineByKey也就是聚合的条件Key，可以是任意类型。后面的V，C两个泛型是需要聚合的值的类型，和聚合后的值的类型，两个类型是可以一样，也可以不一样，例如，Spark中用的多的reduceByKey这个方法，若聚合前的值为long，那么聚合后仍为long。再比如groupByKey，若聚合前为String，那么聚合后为Iterable<String>。
* 再看三个自定义方法：
*  1.createCombiner
这个方法会在每个分区上都执行的，而且只要在分区里碰到在本分区里没有处理过的Key，就会执行该方法。执行的结果就是在本分区里得到指定Key的聚合类型C（可以是数组，也可以是一个值，具体还是得看方法的定义了。）
*  2. mergeValue
这方法也会在每个分区上都执行的，和createCombiner不同，它主要是在分区里碰到在本分区内已经处理过的Key才执行该方法，执行的结果就是将目前碰到的Key的值聚合到已有的聚合类型C中。
其实方法1和2放在一起看，就是一个if判断条件，进来一个Key，就去判断一下若以前没出现过就执行方法1，否则执行方法2.
*  3. mergeCombiner
前两个方法是实现分区内部的相同Key值的数据合并，而这个方法主要用于分区间的相同Key值的数据合并，形成最终的结果。
*  4.partitioner：指定分区函数
*  5.numPartitions：指定分区数
