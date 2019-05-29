## API-1：coalesce，repartition，randomSplit，glom
### 1. coalesce和repartition(重新分区）：
* 源码API
```scala
def coalesce(numPartitions : scala.Int) : org.apache.spark.api.java.JavaRDD[T] = { /* compiled code */ }
def coalesce(numPartitions : scala.Int, shuffle : scala.Boolean) : org.apache.spark.api.java.JavaRDD[T] = { /* compiled code */ }
```
```scala
def repartition(numPartitions : scala.Int) : org.apache.spark.api.java.JavaRDD[T] = { /* compiled code */ }
```
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/CoalesceRepartition.java) 
```java
@Test
    public void coalesceRepartition() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("aa", "aa", "bb", "cc", "dd"), 2);
        System.out.println("原分区：" + RDD1.partitions().size());
        JavaRDD<String> RDD2 = RDD1.coalesce(1);
        System.out.println("coalesce分区：" + RDD2.partitions().size());
        JavaRDD<String> RDD3 = RDD2.repartition(3);
        System.out.println("repartition分区：" + RDD3.partitions().size());
    }
```
* coalesce和repartition都是对RDD进行重新分区，coalesce操作使用HashPartition惊醒分区，第一个参数为重新分区的数目，第二个为是否进行shuffle，默认情况为false
*repartition操作是coalesce函数第二个参数为true的实现
### 2. randomSplit（RDD分片）
根据weights权重将一个RDD分割为多个RDD，返回一个RDD数组  
* 源码API
```scala
def randomSplit(weights : scala.Array[scala.Double]) : scala.Array[org.apache.spark.api.java.JavaRDD[T]] = { /* compiled code */ }
def randomSplit(weights : scala.Array[scala.Double], seed : scala.Long) : scala.Array[org.apache.spark.api.java.JavaRDD[T]] = { /* compiled code */ }
```
* weights: 是一个数组 
根据weight（权重值）将一个RDD划分成多个RDD,权重越高划分得到的元素较多的几率就越大。数组的长度即为划分成RDD的数量,
* seed: 是可选参数 ，作为random的种子,据种子构造出一个Random类。 
* seed 是种子的意思，因为在电脑中实际上是无法产生真正的随机数的， 都是根据给定的种子（通常是当前时间、上几次运算的结果等），通过一个固定的计算公式来得到 下一个随机数seed就是要求使用固定的种子来开始生成随机数。在给定相同的种子下，生成的随机数序列总是相同的
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/RandomSplit.java) 
```java
 @Test
    public void randomSplit() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9"));
        JavaRDD<String>[] rdds = RDD1.randomSplit(new double[]{0.33, 0.22, 0.44});
        for (int i = 0; i < rdds.length; i++) {
            final int index = i;
            JavaRDD rddItem = rdds[i];
            rddItem.collect().forEach(e -> System.out.print("分片【" + index + "】:" + e + ","));
        }
    }
```
### 3. glom（RDD分片）
该函数将RDD中每一个分区中类型为T的元素转换成Array[T]，这样RDD的每个分区就分别只有一个数组元素，返回一个新的RDD，RDD内的元素就是每个分区生成的数组  
* 源码API
```scala
def glom() : org.apache.spark.api.java.JavaRDD[java.util.List[T]] = { /* compiled code */ }
```
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/Glom.java) 
```java
@Test
    public void glom() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9"), 3);
        JavaRDD<List<String>> rddItem = RDD1.glom();
        System.out.println("glom后：" + rddItem.collect());
    }
```
