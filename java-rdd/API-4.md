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
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/CombineByKey.java) 
```java
  @Test
    public void combineByKey() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        JavaPairRDD<String, String> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, String>(s, s + "1"));
        JavaPairRDD<String, String> RDD3 = RDD2.combineByKey(s -> s + "v", (s1, s2) -> s1 + "合并" + s2, (s1, s2) -> s1 + "分区" + s2);
        System.out.println("combineByKey：" + RDD3.collect());
    }
------------------结果---------------
[(6,61v分区61v), (3,31v), (4,41v合并41), (7,71v), (1,11v), (8,81v), (2,21v)]
```
* 需要传入三个自定义的方法和分区属性，分别作用如下：
* 首先紧跟着Aggregator的三个泛型，第一个K，这个是你进行combineByKey也就是聚合的条件Key，可以是任意类型。后面的V，C两个泛型是需要聚合的值的类型，和聚合后的值的类型，两个类型是可以一样，也可以不一样，例如，Spark中用的多的reduceByKey这个方法，若聚合前的值为long，那么聚合后仍为long。再比如groupByKey，若聚合前为String，那么聚合后为Iterable<String>。
* ⭐createCombiner：
这个方法会在每个分区上都执行的，而且只要在分区里碰到在本分区里没有处理过的Key，就会执行该方法。执行的结果就是在本分区里得到指定Key的聚合类型C（可以是数组，也可以是一个值，具体还是得看方法的定义了。）
* ⭐mergeValue：
这方法也会在每个分区上都执行的，和createCombiner不同，它主要是在分区里碰到在本分区内已经处理过的Key才执行该方法，执行的结果就是将目前碰到的Key的值聚合到已有的聚合类型C中。
* 其实方法createCombiner和mergeValue放在一起看，就是一个if判断条件，进来一个Key，就去判断一下若以前没出现过就执行方法createCombiner，否则执行方法mergeValue  
* ⭐mergeCombiner：前两个方法是实现分区内部的相同Key值的数据合并，而这个方法主要用于分区间的相同Key值的数据合并，形成最终的结果。
* partitioner：指定分区函数
* numPartitions：指定分区数
* 说明：最后返回的K是遍历所有K后留下的不重复的K，原RDD重复的K会被上面3个方法整合成一个K，主要是对同一个K的V进行首次创建、分区内计算合并、分区之间计算合并
### 6. foldByKey：
* 源码API:
```scala
def foldByKey(zeroValue : V, partitioner : org.apache.spark.Partitioner, func : org.apache.spark.api.java.function.Function2[V, V, V]) : org.apache.spark.api.java.JavaPairRDD[K, V] = { /* compiled code */ }
def foldByKey(zeroValue : V, numPartitions : scala.Int, func : org.apache.spark.api.java.function.Function2[V, V, V]) : org.apache.spark.api.java.JavaPairRDD[K, V] = { /* compiled code */ }
def foldByKey(zeroValue : V, func : org.apache.spark.api.java.function.Function2[V, V, V]) : org.apache.spark.api.java.JavaPairRDD[K, V] = { /* compiled code */ }
```
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/FoldByKey.java) 
```java
    @Test
    public void foldByKey() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        JavaPairRDD<String, String> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, String>(s, s + "1"));
        JavaPairRDD<String, String> RDD3 = RDD2.foldByKey("start", (s1, s2) -> s1 + "_" + s2);
        System.out.println("foldByKey：" + RDD3.collect());
    }
------------------结果---------------
[(6,start_61_start_61), (3,start_31), (4,start_41_41), (7,start_71), (1,start_11), (8,start_81), (2,start_21)]
```
* 该函数用于RDD[K,V]根据K将V做折叠、合并处理，其中的参数zeroValue表示先根据映射函数将zeroValue应用于V,进行初始化V,再将映射函数应用于初始化后的V
* foldByKey开始折叠的第一个元素不是集合中的第一个元素，而是传入的一个元素 
* zeroValue:每个K的初始值
* partitioner：分区函数
* numPartitions：分区数
* func：重复K的V值计算函数，第一个入参是K当前的V，第二个入参是当前遍历到需要操作的同一个K的另一个V
* 注意：分区内合并的函数和分区间合并函数都是func这同一个  
### 7. reduceByKey（基于combineByKey实现）
  * 源码API:
```scala
def reduceByKey(func : org.apache.spark.api.java.function.Function2[V, V, V]) : org.apache.spark.api.java.JavaPairRDD[K, V] = { /* compiled code */ }
def reduceByKey(partitioner : org.apache.spark.Partitioner, func : org.apache.spark.api.java.function.Function2[V, V, V]) : org.apache.spark.api.java.JavaPairRDD[K, V] = { /* compiled code */ }
//合并本地数据（传入的参数map）
def reduceByKeyLocally(func : org.apache.spark.api.java.function.Function2[V, V, V]) : java.util.Map[K, V] = { /* compiled code */ }
```
* func：同一K的所有V合并函数
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/ReduceByKey.java) 
```java
    @Test
    public void reduceByKey() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        JavaPairRDD<String, String> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, String>(s, s + "1"));
        JavaPairRDD<String, String> RDD3 = RDD2.reduceByKey((s1, s2) -> s1 + "_" + s2);
        System.out.println("reduceByKey：" + RDD3.collect());
    }
---------------------------结果----------------------
[(6,61_61), (3,31), (4,41_41), (7,71), (1,11), (8,81), (2,21)]
```
### 8. groupByKey（基于combineByKey实现）：
* 源码API:
```scala
def groupByKey() : org.apache.spark.api.java.JavaPairRDD[K, java.lang.Iterable[V]] = { /* compiled code */ }
def groupByKey(partitioner : org.apache.spark.Partitioner) : org.apache.spark.api.java.JavaPairRDD[K, java.lang.Iterable[V]] = { /* compiled code */ }
def groupByKey(numPartitions : scala.Int) : org.apache.spark.api.java.JavaPairRDD[K, java.lang.Iterable[V]] = { /* compiled code */ }
```
* groupByKey会将RDD[key,value] 按照相同的key进行分组，形成RDD[key,Iterable[value]]的形式， 有点类似于sql中的groupby，例如类似于mysql中的group_concat 
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/GroupByKey.java) 
```java
    @Test
    public void groupByKey() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        JavaPairRDD<String, Integer> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
        JavaPairRDD<String, Iterable<Integer>> RDD3 = RDD2.groupByKey();
        System.out.println("groupByKey：" + RDD3.collect());
    }
---------------结果----------------------
[(6,[1, 1]), (3,[1]), (4,[1, 1]), (7,[1]), (1,[1]), (8,[1]), (2,[1])]
```
### 9. sortByKey（基于combineByKey实现）：
* 源码API:
```scala
def sortByKey() : org.apache.spark.api.java.JavaPairRDD[K, V] = { /* compiled code */ }
def sortByKey(ascending : scala.Boolean) : org.apache.spark.api.java.JavaPairRDD[K, V] = { /* compiled code */ }
def sortByKey(ascending : scala.Boolean, numPartitions : scala.Int) : org.apache.spark.api.java.JavaPairRDD[K, V] = { /* compiled code */ }
def sortByKey(comp : java.util.Comparator[K]) : org.apache.spark.api.java.JavaPairRDD[K, V] = { /* compiled code */ }
def sortByKey(comp : java.util.Comparator[K], ascending : scala.Boolean) : org.apache.spark.api.java.JavaPairRDD[K, V] = { /* compiled code */ }
def sortByKey(comp : java.util.Comparator[K], ascending : scala.Boolean, numPartitions : scala.Int) : org.apache.spark.api.java.JavaPairRDD[K, V] = { /* compiled code */ }
```
* SortByKey用于对pairRDD按照key进行排序，第一个参数可以设置true或者false，默认是true 
* comp :排序计算函数
* ascending：true：升序排列（默认），false：降序排列
* 注意：序列化问题,这里api中是需要实现java的java.util.Comparator,但是这个接口没有实现序列化，我们需要自己写一个实现类去实现java.util.Comparator同时实现序列化，注意序列化的类如果私用java的java.io.Serializable有时候会报错，可以使用scala的scala.Serializable 
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/SortByKey.java) 
```java
     @Test
    public void sortByKey() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        JavaPairRDD<String, Integer> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
        class SecondaryComparator implements Comparator<String>, Serializable {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        }
        System.out.println("排序前分区数：" + RDD2.partitions().size());
        JavaPairRDD<String, Integer> RDD3 = RDD2.sortByKey(new SecondaryComparator(), true, 1);
        System.out.println("排序后分区数：" + RDD3.partitions().size());
        System.out.println("sortByKey：" + RDD3.collect());
    }
--------------结果------------------------------
[(1,1), (2,1), (3,1), (4,1), (4,1), (6,1), (6,1), (7,1), (8,1)]
```
### 10. cogroup：
* 源码API:
```scala
def cogroup[W](other : org.apache.spark.api.java.JavaPairRDD[K, W], partitioner : org.apache.spark.Partitioner) : org.apache.spark.api.java.JavaPairRDD[K, scala.Tuple2[java.lang.Iterable[V], java.lang.Iterable[W]]] = { /* compiled code */ }
def cogroup[W1, W2](other1 : org.apache.spark.api.java.JavaPairRDD[K, W1], other2 : org.apache.spark.api.java.JavaPairRDD[K, W2], partitioner : org.apache.spark.Partitioner) : org.apache.spark.api.java.JavaPairRDD[K, scala.Tuple3[java.lang.Iterable[V], java.lang.Iterable[W1], java.lang.Iterable[W2]]] = { /* compiled code */ }
def cogroup[W1, W2, W3](other1 : org.apache.spark.api.java.JavaPairRDD[K, W1], other2 : org.apache.spark.api.java.JavaPairRDD[K, W2], other3 : org.apache.spark.api.java.JavaPairRDD[K, W3], partitioner : org.apache.spark.Partitioner) : org.apache.spark.api.java.JavaPairRDD[K, scala.Tuple4[java.lang.Iterable[V], java.lang.Iterable[W1], java.lang.Iterable[W2], java.lang.Iterable[W3]]] = { /* compiled code */ }
def cogroup[W](other : org.apache.spark.api.java.JavaPairRDD[K, W]) : org.apache.spark.api.java.JavaPairRDD[K, scala.Tuple2[java.lang.Iterable[V], java.lang.Iterable[W]]] = { /* compiled code */ }
def cogroup[W1, W2](other1 : org.apache.spark.api.java.JavaPairRDD[K, W1], other2 : org.apache.spark.api.java.JavaPairRDD[K, W2]) : org.apache.spark.api.java.JavaPairRDD[K, scala.Tuple3[java.lang.Iterable[V], java.lang.Iterable[W1], java.lang.Iterable[W2]]] = { /* compiled code */ }
def cogroup[W1, W2, W3](other1 : org.apache.spark.api.java.JavaPairRDD[K, W1], other2 : org.apache.spark.api.java.JavaPairRDD[K, W2], other3 : org.apache.spark.api.java.JavaPairRDD[K, W3]) : org.apache.spark.api.java.JavaPairRDD[K, scala.Tuple4[java.lang.Iterable[V], java.lang.Iterable[W1], java.lang.Iterable[W2], java.lang.Iterable[W3]]] = { /* compiled code */ }
def cogroup[W](other : org.apache.spark.api.java.JavaPairRDD[K, W], numPartitions : scala.Int) : org.apache.spark.api.java.JavaPairRDD[K, scala.Tuple2[java.lang.Iterable[V], java.lang.Iterable[W]]] = { /* compiled code */ }
def cogroup[W1, W2](other1 : org.apache.spark.api.java.JavaPairRDD[K, W1], other2 : org.apache.spark.api.java.JavaPairRDD[K, W2], numPartitions : scala.Int) : org.apache.spark.api.java.JavaPairRDD[K, scala.Tuple3[java.lang.Iterable[V], java.lang.Iterable[W1], java.lang.Iterable[W2]]] = { /* compiled code */ }
def cogroup[W1, W2, W3](other1 : org.apache.spark.api.java.JavaPairRDD[K, W1], other2 : org.apache.spark.api.java.JavaPairRDD[K, W2], other3 : org.apache.spark.api.java.JavaPairRDD[K, W3], numPartitions : scala.Int) : org.apache.spark.api.java.JavaPairRDD[K, scala.Tuple4[java.lang.Iterable[V], java.lang.Iterable[W1], java.lang.Iterable[W2], java.lang.Iterable[W3]]] = { /* compiled code */ }
```
* groupByKey是对单个 RDD 的数据进行分组，还可以使用一个叫作 cogroup() 的函数对多个共享同一个键的 RDD 进行分组 
* 例如 RDD1.cogroup(RDD2) 会将RDD1和RDD2按照相同的key进行分组，得到(key,RDD[key,Iterable[value1],Iterable[value2]])的形式 
cogroup也可以多个进行分组 
* 例如RDD1.cogroup(RDD2,RDD3,…RDDN), 可以得到(key,Iterable[value1],Iterable[value2],Iterable[value3],…,Iterable[valueN]) 
* 案例,scoreDetail存放的是学生的优秀学科的分数，scoreDetai2存放的是刚刚及格的分数，scoreDetai3存放的是没有及格的科目的分数，我们要对每一个学生的优秀学科，刚及格和不及格的分数给分组统计出来 
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/Cogroup.java) 
```java
     @Test
    public void cogroup() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<Tuple2<String, Float>> scoreDetails1 = javaSparkContext.parallelize(Arrays.asList(new Tuple2("xiaoming", 75)
                , new Tuple2("xiaoming", 90)
                , new Tuple2("lihua", 95)
                , new Tuple2("lihua", 96)));
        JavaRDD<Tuple2<String, Float>> scoreDetails2 = javaSparkContext.parallelize(Arrays.asList(new Tuple2("xiaoming", 75)
                , new Tuple2("lihua", 60)
                , new Tuple2("lihua", 62)));
        JavaRDD<Tuple2<String, Float>> scoreDetails3 = javaSparkContext.parallelize(Arrays.asList(new Tuple2("xiaoming", 75)
                , new Tuple2("xiaoming", 45)
                , new Tuple2("lihua", 24)
                , new Tuple2("lihua", 57)));

        JavaPairRDD<String, Float> scoreMapRDD1 = JavaPairRDD.fromJavaRDD(scoreDetails1);
        JavaPairRDD<String, Float> scoreMapRDD2 = JavaPairRDD.fromJavaRDD(scoreDetails2);
        JavaPairRDD<String, Float> scoreMapRDD3 = JavaPairRDD.fromJavaRDD(scoreDetails2);
        JavaPairRDD<String, Tuple3<Iterable<Float>, Iterable<Float>, Iterable<Float>>> cogroupRDD = scoreMapRDD1.cogroup(scoreMapRDD2, scoreMapRDD3);
        Map<String, Tuple3<Iterable<Float>, Iterable<Float>, Iterable<Float>>> tuple3 = (Map<String, Tuple3<Iterable<Float>, Iterable<Float>, Iterable<Float>>>) cogroupRDD.collectAsMap();
        for (String key : tuple3.keySet()) {
            System.out.println("数据：(" + key + ", " + tuple3.get(key) + ")");
        }
    }
---------------------------结果--------------------------------
(lihua, ([95, 96],[60, 62],[60, 62]))
(xiaoming, ([75, 90],[75],[75]))
```
出参说明：
* 1.出参是一个JavaPairRDD，K是RDD中的K
* 2.value是一个Tuple对象，在sacala中Tuple对象可以存N个值，对应的类我TupleN，在这个Tuple中，每个参与cogroup计算的RDD，会按照入参的先后顺序，列出和K对应的所有值的Iterable对象，当然调用该方法的RDD肯定是放在第一个
