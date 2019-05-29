## API-2：distinct，union，intersection，subtract，cartesian
### 1. distinct（去重，此方法涉及到混洗，操作开销很大）
* 源码API
```scala
//去重
def distinct() : org.apache.spark.api.java.JavaRDD[T] = { /* compiled code */ }
//去重并指定分区数
def distinct(numPartitions : scala.Int) : org.apache.spark.api.java.JavaRDD[T] = { /* compiled code */ }
```
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/Distinct.java) 
```java
    @Test
    public void distinct() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = javaSparkContext.parallelize(Arrays.asList("sssss", "sssss", "zzzz", "zzzz", "xyzzz"));
        JavaRDD<String> distinctRdd = rdd.distinct();
        distinctRdd.collect().forEach(e -> System.out.println("去重后:" + e));
    }
```
* distinct用于去重， 我们生成的RDD可能有重复的元素，使用distinct方法可以去掉重复的元素, 不过此方法涉及到混洗，操作开销很大
### 2. union(两个RDD进行合并)
* 源码API
```scala
def union(other : org.apache.spark.api.java.JavaRDD[T]) : org.apache.spark.api.java.JavaRDD[T] = { /* compiled code */ }
```
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/Union.java) 
```java
JavaRDD<String> RDD1 = sc.parallelize(Arrays.asList("aa", "aa", "bb", "cc", "dd"));
JavaRDD<String> RDD2 = sc.parallelize(Arrays.asList("aa","dd","ff"));
JavaRDD<String> unionRDD = RDD1.union(RDD2);
List<String> collect = unionRDD.collect();
for (String str:collect) {
    System.out.print(str+", ");
}
-----------输出---------
aa, aa, bb, cc, dd, aa, dd, ff,
```

### 3. intersection（返回两个RDD的交集，并且去重 ）
* 源码API
```scala
def intersection(other : org.apache.spark.api.java.JavaRDD[T]) : org.apache.spark.api.java.JavaRDD[T] = { /* compiled code */ }
```
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/Intersection.java) 
```java
JavaRDD<String> RDD1 = sc.parallelize(Arrays.asList("aa", "aa", "bb", "cc", "dd"));
JavaRDD<String> RDD2 = sc.parallelize(Arrays.asList("aa","dd","ff"));
JavaRDD<String> intersectionRDD = RDD1.intersection(RDD2);
List<String> collect = intersectionRDD.collect();
for (String str:collect) {
    System.out.print(str+" ");
}
-------------输出-----------
aa dd
```
* intersection 需要混洗数据，比较浪费性能 
### 4. subtract(返回在RDD1中出现，但是不在RDD2中出现的元素，不去重)
* 源码API
```scala
def subtract(other : org.apache.spark.api.java.JavaRDD[T]) : org.apache.spark.api.java.JavaRDD[T] = { /* compiled code */ }
//新生成RDD指定分区数
def subtract(other : org.apache.spark.api.java.JavaRDD[T], numPartitions : scala.Int) : org.apache.spark.api.java.JavaRDD[T] = { /* compiled code */ }
//新生成RDD指定分区函数
def subtract(other : org.apache.spark.api.java.JavaRDD[T], p : org.apache.spark.Partitioner) : org.apache.spark.api.java.JavaRDD[T] = { /* compiled code */ }
``` 
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/Subtract.java) 
```java
JavaRDD<String> RDD1 = sc.parallelize(Arrays.asList("aa", "aa", "bb","cc", "dd"));
JavaRDD<String> RDD2 = sc.parallelize(Arrays.asList("aa","dd","ff"));
JavaRDD<String> subtractRDD = RDD1.subtract(RDD2);
List<String> collect = subtractRDD.collect();
for (String str:collect) {
    System.out.print(str+" ");
}
------------输出-----------------
bb  cc 
```
### 5. cartesian(返回RDD1和RDD2的笛卡儿积，这个开销非常大)
* 源码API
```scala
def cartesian[U](other : org.apache.spark.api.java.JavaRDDLike[U, _]) : org.apache.spark.api.java.JavaPairRDD[T, U] = { /* compiled code */ }
``` 
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/Cartesian.java) 
```java
JavaRDD<String> RDD1 = sc.parallelize(Arrays.asList("1", "2", "3"));
    JavaRDD<String> RDD2 = sc.parallelize(Arrays.asList("a","b","c"));
    JavaPairRDD<String, String> cartesian = RDD1.cartesian(RDD2);

    List<Tuple2<String, String>> collect1 = cartesian.collect();
    for (Tuple2<String, String> tp:collect1) {
        System.out.println("("+tp._1+" "+tp._2+")");
    }
------------输出-----------------
(1 a)
(1 b)
(1 c)
(2 a)
(2 b)
(2 c)
(3 a)
(3 b)
(3 c)
```
