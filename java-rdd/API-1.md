## API：map，flatMap，filter，mapPartitons，mapToPair，flatMapToPair
### 1. map方法如下：
* 源码API
```scala
def map[R](f : org.apache.spark.api.java.function.Function[T, R]) : org.apache.spark.api.java.JavaRDD[R] = { /* compiled code */ }
```
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/RddMap.java) 
```java
    @Test
    public void map() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = javaSparkContext.parallelize(Arrays.asList("klhk lsad has", "dfsdf sdf", "sdgg hgfh", "yu yds f", "cxvx cvasd"));
        JavaRDD<String> mapedRdd = rdd.map(e -> e + "_我被map处理过");
        mapedRdd.collect().forEach(e -> System.out.println(e));
    }
```
* map操作是对RDD中的每个元素都执行一个指定的函数来生成一个新的RDD，任何原RDD中的元素在新的RDD中都有且只有一个元素与之对应。
### 2. flatMap方法如下：
* 源码API
```scala
def flatMap[U](f : org.apache.spark.api.java.function.FlatMapFunction[T, U]) : org.apache.spark.api.java.JavaRDD[U] = { /* compiled code */ }
def flatMapToDouble(f : org.apache.spark.api.java.function.DoubleFlatMapFunction[T]) : org.apache.spark.api.java.JavaDoubleRDD = { /* compiled code */ }
def flatMapToPair[K2, V2](f : org.apache.spark.api.java.function.PairFlatMapFunction[T, K2, V2]) : org.apache.spark.api.java.JavaPairRDD[K2, V2] = { /* compiled code */ }
```
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/FlatMap.java) 
```java
    @Test
    public void flatMap() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<Integer> rdd = javaSparkContext.parallelize(Arrays.asList(2, 23, 12, 312, 312, 3, 123, 123, 14, 32, 54, 5, 123, 123, 235, 23, 51));
        JavaRDD<String> mapedRdd = rdd.flatMap(e -> {
            ArrayList<String> result = new ArrayList();
            if (e > 20) {
                result.add("字符：" + e);
            }
            return result.iterator();
        });
        mapedRdd.collect().forEach(e -> System.out.println(e));
    }
```
* 与map类似，区别是原RDD中元素经过flatMap内函数处理后会生成一个Iterator迭代器，而不是一个和原RDD存储的一样的对象，Iterator可以放一个或者多个值，方法返回值会将所有Iterator中的数据拿出来，放到同一个RDD中。
* flatMapToDouble：返回的RDD中的对象是Double类型
* flatMapToPair：返回的PairRdd
### 3. filter方法如下：
```scala
def filter(f : org.apache.spark.api.java.function.Function[T, java.lang.Boolean]) : org.apache.spark.api.java.JavaRDD[T] = { /* compiled code */ }
```
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/Filter.java) 
```java
     @Test
    public void filter() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = javaSparkContext.parallelize(Arrays.asList("klhk lsad has", "sdfsdf sdf", "sdgg hgfh", "yu yds f", "cxvx cvasd"));
        JavaRDD<String> filterRdd = rdd.filter(e -> e.startsWith("s"));
        filterRdd.collect().forEach(e -> System.out.println("s开头才算哦:" + e));
    }
```
* 使用filter中的函数过滤数据，只有函数结果返回true的值才会放入新的RDD中

### 4. mapPartitions方法如下：
```scala
def mapPartitions[U](f : org.apache.spark.api.java.function.FlatMapFunction[java.util.Iterator[T], U]) : org.apache.spark.api.java.JavaRDD[U] = { /* compiled code */ }
def mapPartitions[U](f : org.apache.spark.api.java.function.FlatMapFunction[java.util.Iterator[T], U], preservesPartitioning : scala.Boolean) : org.apache.spark.api.java.JavaRDD[U] = { /* compiled code */ }
def mapPartitionsToDouble(f : org.apache.spark.api.java.function.DoubleFlatMapFunction[java.util.Iterator[T]]) : org.apache.spark.api.java.JavaDoubleRDD = { /* compiled code */ }
def mapPartitionsToPair[K2, V2](f : org.apache.spark.api.java.function.PairFlatMapFunction[java.util.Iterator[T], K2, V2]) : org.apache.spark.api.java.JavaPairRDD[K2, V2] = { /* compiled code */ }
def mapPartitionsToDouble(f : org.apache.spark.api.java.function.DoubleFlatMapFunction[java.util.Iterator[T]], preservesPartitioning : scala.Boolean) : org.apache.spark.api.java.JavaDoubleRDD = { /* compiled code */ }
def mapPartitionsToPair[K2, V2](f : org.apache.spark.api.java.function.PairFlatMapFunction[java.util.Iterator[T], K2, V2], preservesPartitioning : scala.Boolean) : org.apache.spark.api.java.JavaPairRDD[K2, V2] = { /* compiled code */ }
def mapPartitionsWithIndex[R](f : org.apache.spark.api.java.function.Function2[java.lang.Integer, java.util.Iterator[T], java.util.Iterator[R]], preservesPartitioning : scala.Boolean = { /* compiled code */ }) : org.apache.spark.api.java.JavaRDD[R] = { /* compiled code */ }
```
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/MapPartitions.java) 
```java
    @Test
    public void mapPartitions() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9"), 3);
        JavaRDD<String> RDD2 = RDD1.mapPartitions(stringIterator -> {
            ArrayList<String> results = new ArrayList<>();
            while (stringIterator.hasNext()) {
                String item = stringIterator.next();
                results.add("处理过咯：" + item);
            }
            return results.iterator();
        });
        System.out.println("mapPartitions后：" + RDD2.collect());
    }
```
* 和Map操作类似，只不过这里说对每个分区进行处理，在函数中使用分区迭代器处理分区中的每个元素，其中参数preservesPartitioning标识是否保留父亲RDD栋partition分区信息
* mapPartitions比较适合需要分批处理数据的情况，比如将数据插入某个表，每批数据只需要开启一次数据库连接，大大减少了连接开支
* mapPartitionsWithIndex：操作和mapPartitions类似，只是函数的输入参数多了一个分区索引
### 5. mapToPail方法如下：
```scala
def mapToPair[K2, V2](f : org.apache.spark.api.java.function.PairFunction[T, K2, V2]) : org.apache.spark.api.java.JavaPairRDD[K2, V2] = { /* compiled code */ }
```
* 和map类似，逐个遍历rdd中的元素，使用函数生成Tuple2对，Tuple2对的个数和原RDD一致
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/MapToPail.java) 
```java
    @Test
    public void mapToPail() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9"), 3);
        JavaPairRDD<String, String> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, String>(s, s));
        System.out.println("mapToPail后：" + RDD2.collect());
    }
 ```

### 6. flatMapToPair
* 和flatmap类似，只不过返回的是Tuple2对象，RDD是PairRdd
