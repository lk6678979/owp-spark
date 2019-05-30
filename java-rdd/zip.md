## ZIP：zip，zipPartitions
### 1. zip：
* 源码API
```scala
def zip[U](other : org.apache.spark.api.java.JavaRDDLike[U, _]) : org.apache.spark.api.java.JavaPairRDD[T, U] = { /* compiled code */ }
def zipWithUniqueId() : org.apache.spark.api.java.JavaPairRDD[T, java.lang.Long] = { /* compiled code */ }
def zipWithIndex() : org.apache.spark.api.java.JavaPairRDD[T, java.lang.Long] = { /* compiled code */ }
```
* 自身的RDD的值的类型为T类型，另一个RDD的值的类型为U类型。zip操作将这两个值连接在一起。构成一个元祖值。RDD的值的类型为元祖（返回JavaPairRDD） 
都是第i个值和第i个值进行连接。  
* zip函数用于将两个RDD组合成Key/Value形式的RDD,这里默认两个RDD的partition数量以及元素数量都相同，否则会抛出异常  
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/Zip.java) 
```java
    @Test
    public void zip() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9"), 3);
        JavaRDD<Integer> RDD2 = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9), 3);
        JavaPairRDD rdds = RDD1.zip(RDD2);
        System.out.println("zip后：" + rdds.collect());
    }
---------------------------结果------------------------
[(1,1), (2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8), (9,9)]
```
* zipWithUniqueId：返回PairRDD，第一个值是原RDD的值，第二个值是uniquedId
* zipWithIndex：返回PairRDD，第一个值是原RDD的值，第二个值是index
### 2. zipPartitions：
* 源码API
```scala
def zipPartitions[U, V](other : org.apache.spark.api.java.JavaRDDLike[U, _], f : org.apache.spark.api.java.function.FlatMapFunction2[java.util.Iterator[T], java.util.Iterator[U], V]) : org.apache.spark.api.java.JavaRDD[V] = { /* compiled code */ }
```
* zipPartitions函数将两个RDD按照partition组合成为新的RDD，该函数需要组合的RDD具有相同的分区数，但对于每个分区内的元素数量没有要求。 
* preservesPartitioning表示的是否保留父RDD的partitioner分区信息。 
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/ZipPartitions.java) 
```java
    @Test
    public void zipPartitions() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9"), 3);
        JavaRDD<Integer> RDD2 = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);
        JavaRDD<String> zipPartitionsRDD = RDD1.zipPartitions(RDD2, (integerIterator, integerIterator2) -> {
            List<String> arrayList = new ArrayList<>();
            while (integerIterator.hasNext() && integerIterator2.hasNext())
                arrayList.add(integerIterator.next().toString() + "_" + integerIterator2.next().toString());
            return arrayList.iterator();
        });
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" + zipPartitionsRDD.collect());
    }
-------------------------------结果--------------------------------
[1_1, 2_2, 3_3, 4_4, 5_5, 6_6, 7_7, 8_8, 9_9]
```
说明：就是将2个RDD相同index的partition作为入参放到函数中，然后自己去生成新的RDD的元素Iterato
