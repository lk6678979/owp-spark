# SPARK的JAVA版API案例
## 1.创建RDD
### 1.1使用已存在的集合创建RDD（自己创建数据生成RDD）[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/Parallelize.java)   
#### 1.1.1  parallelize（使用集合创建RDD）
* 源码API
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
** JAVA API
```java
@Test
    //使用集合创建RDD
    public void parallelize() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<String> datas = new ArrayList<>();
        datas.add("213123123 12314124123 435435");
        datas.add("5654645 32432432");
        datas.add("678768");
        datas.add("345435");
        datas.add("43534 655123");
        datas.add("90980");
        datas.add("54654");
        datas.add("12312314");
        JavaRDD<String> rdd = javaSparkContext.parallelize(datas, 2);
        //获取所有字（空格分隔的字）
        JavaRDD<String> words = rdd.flatMap(e -> Arrays.asList(e.split(" ")).iterator());
        //将RDD的数据组装成对
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(e -> new Tuple2<>(e, 1));
        //将成对的数据进行聚合
        JavaPairRDD<String, Integer> wordReduced = wordAndOne.reduceByKey((integer, integer2) -> integer + integer2);
        //java只支持按照key进行排序,我们先将Paer中kv呼唤
        JavaPairRDD<Integer, String> swaped = wordReduced.mapToPair(stringIntegerTuple2 -> stringIntegerTuple2.swap());
        //使用key排序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey();
        sorted.saveAsTextFile("C:\\Users\\xxx\\Desktop\\90\\baseInfo2.log");
        javaSparkContext.stop();
    }
```
