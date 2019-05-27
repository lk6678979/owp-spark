## 1.创建RDD
### 1.1 使用已存在的集合创建RDD（自己创建数据生成RDD）
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
#### 1.1.1 parallelize（使用集合创建RDD）[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/Parallelize.java)   
* JAVA API
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
#### 1.1.2 parallelizePairs（使用集合创建PairRDD）[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/ParallelizePairs.java)   
* JAVA API
```java
@Test
    public void parallelizePairs() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        List<Tuple2<String, Integer>> tuples = new ArrayList<>();
        tuples.add(new Tuple2("2220", 1));
        tuples.add(new Tuple2("2220", 1));
        tuples.add(new Tuple2("435345", 1));
        tuples.add(new Tuple2("212312220", 1));
        tuples.add(new Tuple2("2220", 1));
        tuples.add(new Tuple2("224353420", 1));
        tuples.add(new Tuple2("2213213120", 1));
        //将RDD的数据组装成对
        JavaPairRDD<String, Integer> wordAndOne = javaSparkContext.parallelizePairs(tuples, 3);
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
### 1.2 makeRDD（创建RDD,并可以指定分区的首选节点位置）,` 只有scala版本的才有makeRDD ,可以指定每一个分区的首选节点位置 `  
* 源码API
```scala
def makeRDD[T](seq : scala.Seq[T], numSlices : scala.Int = { /* compiled code */ }) 
```
### 1.3 使用外部存储创建RDD（支持HDFS、Cassandra、Hbase、Amazon等）
#### 1.3.1 textFile（读取本地文件或者HDFS文件转为RDD，支持文本文件、SequenceFiles和任何Hadoop InputFormat格式）
```scala
//读取外部文件生成RDD，path:参数是读取目录
def textFile(path : scala.Predef.String) : org.apache.spark.api.java.JavaRDD[scala.Predef.String] = { /* compiled code */ }
//读取外部文件生成RDD，path:参数是读取目录，minPartitions：分区数
def textFile(path : scala.Predef.String, minPartitions : scala.Int) : org.apache.spark.api.java.JavaRDD[scala.Predef.String] = { /* compiled code */ }
```
##### 特性：
* 该操作支持整个文件目录读取，文件可以是文本或者压缩文件，如gzip等自动执行解压缩并加载数据）
* 将path 里的所有文件内容读出，以文件中的每一行作为一条记录的方式
* 支持模糊匹配，例如把F:\dataexample\wordcount\目录下inp开头的给转换成RDD
```java
JavaRDD<String> rdd = javaSparkContext.textFile("C:\\local\\es-*")
```
* 多个路径可以使用逗号分隔，例如
```java
JavaRDD<String> rdd = javaSparkContext.textFile("C:\\local\\es-*,C:\\local\\zs-*")
```
* 如果根据路径一个文件都无法找到，会报错
```shell
org.apache.hadoop.mapred.InvalidInputException: Input Pattern file:/C:/local/zs-* matches 0 files
```
* 路径只能指定文件，或者是只包含文件的文件夹，否则报错
```shell
java.io.IOException: Not a file: file:/C:/Users/liukai/Desktop/90/nexus
```

#### 1.3.2 wholeTextFile（读取目录里面所有的文件，并返回（文件全路径，文件内容字符串）对
//读取目录下的文件生成PairRdd,path:参数是读取目录，minPartitions：最小分区数
def wholeTextFiles(path : scala.Predef.String, minPartitions : scala.Int) : org.apache.spark.api.java.JavaPairRDD[scala.Predef.String, scala.Predef.String] = { /* compiled code */ }
//读取目录下的文件生成PairRdd,path:参数是读取目录
def wholeTextFiles(path : scala.Predef.String) : org.apache.spark.api.java.JavaPairRDD[scala.Predef.String, scala.Predef.String] = { /* compiled code */ }

特性：
读取的目录下可以有子目录，但是API不会去读取子目录，只会读取指定目录下的文件（也不会报错）

1.2.2 其他外部文件读取方式
读取二级制文件生成RDD
//path：二进制文件路径，minPartitions：最小分区数
def binaryFiles(path : scala.Predef.String, minPartitions : scala.Int) : org.apache.spark.api.java.JavaPairRDD[scala.Predef.String, org.apache.spark.input.PortableDataStream] = { /* compiled code */ }
//path：二进制文件路径
def binaryFiles(path : scala.Predef.String) : org.apache.spark.api.java.JavaPairRDD[scala.Predef.String, org.apache.spark.input.PortableDataStream] = { /* compiled code */ }
//path：二进制文件路径，recordLength：读取数据长度
def binaryRecords(path : scala.Predef.String, recordLength : scala.Int) : org.apache.spark.api.java.JavaRDD[scala.Array[scala.Byte]] = { /* compiled code */ }

读取SequenceFile生成RDD
//path：文件路径，keyClass：key值的class对象，valueClass：value值的class对象，minPartitions：最小分区数
def sequenceFile[K, V](path : scala.Predef.String, keyClass : scala.Predef.Class[K], valueClass : scala.Predef.Class[V], minPartitions : scala.Int) : org.apache.spark.api.java.JavaPairRDD[K, V] = { /* compiled code */ }
//path：文件路径，keyClass：key值的class对象，valueClass：value值的class对象
def sequenceFile[K, V](path : scala.Predef.String, keyClass : scala.Predef.Class[K], valueClass : scala.Predef.Class[V]) : org.apache.spark.api.java.JavaPairRDD[K, V] = { /* compiled code */ }

读取Hdoop文件生成RDD
def hadoopRDD[K, V, F <: org.apache.hadoop.mapred.InputFormat[K, V]](conf : org.apache.hadoop.mapred.JobConf, inputFormatClass : scala.Predef.Class[F], keyClass : scala.Predef.Class[K], valueClass : scala.Predef.Class[V], minPartitions : scala.Int) : org.apache.spark.api.java.JavaPairRDD[K, V] = { /* compiled code */ }
def hadoopRDD[K, V, F <: org.apache.hadoop.mapred.InputFormat[K, V]](conf : org.apache.hadoop.mapred.JobConf, inputFormatClass : scala.Predef.Class[F], keyClass : scala.Predef.Class[K], valueClass : scala.Predef.Class[V]) : org.apache.spark.api.java.JavaPairRDD[K, V] = { /* compiled code */ }
def hadoopFile[K, V, F <: org.apache.hadoop.mapred.InputFormat[K, V]](path : scala.Predef.String, inputFormatClass : scala.Predef.Class[F], keyClass : scala.Predef.Class[K], valueClass : scala.Predef.Class[V], minPartitions : scala.Int) : org.apache.spark.api.java.JavaPairRDD[K, V] = { /* compiled code */ }
def hadoopFile[K, V, F <: org.apache.hadoop.mapred.InputFormat[K, V]](path : scala.Predef.String, inputFormatClass : scala.Predef.Class[F], keyClass : scala.Predef.Class[K], valueClass : scala.Predef.Class[V]) : org.apache.spark.api.java.JavaPairRDD[K, V] = { /* compiled code */ }
def newAPIHadoopFile[K, V, F <: org.apache.hadoop.mapreduce.InputFormat[K, V]](path : scala.Predef.String, fClass : scala.Predef.Class[F], kClass : scala.Predef.Class[K], vClass : scala.Predef.Class[V], conf : org.apache.hadoop.conf.Configuration) : org.apache.spark.api.java.JavaPairRDD[K, V] = { /* compiled code */ }
def newAPIHadoopRDD[K, V, F <: org.apache.hadoop.mapreduce.InputFormat[K, V]](conf : org.apache.hadoop.conf.Configuration, fClass : scala.Predef.Class[F], kClass : scala.Predef.Class[K], vClass : scala.Predef.Class[V]) : org.apache.spark.api.java.JavaPairRDD[K, V] = { /* compiled code */ }

特性：
由于Hadoop的接口有新旧两个版本，所有Spark为了能够兼容Hadoop版本，也提供了两套创建操作接口。对于外部存储创建操作而言，hadoopRDD和newHadoopRDD是最为抽象的两个函数接口
使用hadoopRDD操作可以将其他任何Hadoop输入类型转化成RDD使用操作
一般来说HadoopRDD中每一个HDFS数据库都成为一个RDD分区
通过转换操作可以将HadoopRDD等转换成FilterRDD（依赖一个父RDD）和JoinedRDD（依赖所有父RDD）
