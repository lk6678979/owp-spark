package com.owp.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.storage.StorageLevel;
import org.junit.Test;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.*;

/**
 * @描述:
 * @公司:
 * @作者:
 * @版本: 1.0.0
 * @日期: 2019-04-05 12:01:12
 */
public class JavaApiDemo implements Serializable {
    @Test
    public void mapValues() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9"), 3);
        JavaPairRDD<String, String> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, String>(s, s));
        JavaPairRDD<String, String> RDD3 = RDD2.mapValues(e -> e + "1");
        System.out.println("mapValues后：" + RDD3.collect());
    }

    @Test
    public void flatMapValues() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9"), 3);
        JavaPairRDD<String, String> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, String>(s, s));
        JavaPairRDD<String, String> RDD3 = RDD2.flatMapValues(s -> Arrays.asList(s, s + "1", s + 2));
        System.out.println("flatMapValues后：" + RDD3.collect());
    }

    @Test
    public void combineByKey() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        JavaPairRDD<String, String> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, String>(s, s + "1"));
        JavaPairRDD<String, String> RDD3 = RDD2.combineByKey(s -> s + "v", (s1, s2) -> s1 + "合并" + s2, (s1, s2) -> s1 + "分区" + s2);
        System.out.println("combineByKey：" + RDD3.collect());
    }

    @Test
    public void foldByKey() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        JavaPairRDD<String, String> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, String>(s, s + "1"));
        JavaPairRDD<String, String> RDD3 = RDD2.foldByKey("start", (s1, s2) -> s1 + "_" + s2);
        System.out.println("foldByKey：" + RDD3.collect());
    }

    @Test
    public void reduceByKey() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        JavaPairRDD<String, String> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, String>(s, s + "1"));
        JavaPairRDD<String, String> RDD3 = RDD2.reduceByKey((s1, s2) -> s1 + "_" + s2);
        System.out.println("reduceByKey：" + RDD3.collect());
    }

    @Test
    public void groupByKey() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        JavaPairRDD<String, Integer> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
        JavaPairRDD<String, Iterable<Integer>> RDD3 = RDD2.groupByKey();
        System.out.println("groupByKey：" + RDD3.collect());
    }

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
//        System.out.println("sortByKey：" + RDD3.collect());
    }

    @Test
    public void subtractByKey() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        JavaPairRDD<String, Integer> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
        JavaRDD<String> RDD3 = javaSparkContext.parallelize(Arrays.asList("2", "3", "6", "7", "6"));
        JavaPairRDD<String, Integer> RDD4 = RDD3.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
        JavaPairRDD<String, Integer> RDD5 = RDD2.subtractByKey(RDD4);
        System.out.println("subtractByKey：" + RDD5.collect());
    }

    @Test
    public void join() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        JavaPairRDD<String, Integer> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
        JavaRDD<String> RDD3 = javaSparkContext.parallelize(Arrays.asList("2", "3", "6", "7", "6"));
        JavaPairRDD<String, Integer> RDD4 = RDD3.mapToPair(s -> new Tuple2<String, Integer>(s, 2));
        JavaPairRDD<String, Tuple2<Integer, Integer>> RDD5 = RDD2.join(RDD4);
        System.out.println("join：" + RDD5.collect());
    }

    @Test
    public void leftOuterJoin() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        JavaPairRDD<String, Integer> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
        JavaRDD<String> RDD3 = javaSparkContext.parallelize(Arrays.asList("2", "3", "6", "7", "6"));
        JavaPairRDD<String, Integer> RDD4 = RDD3.mapToPair(s -> new Tuple2<String, Integer>(s, 2));
        JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> RDD5 = RDD2.leftOuterJoin(RDD4);
        System.out.println("leftOuterJoin：" + RDD5.collect());
    }

    @Test
    public void rightOuterJoin() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        JavaPairRDD<String, Integer> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
        JavaRDD<String> RDD3 = javaSparkContext.parallelize(Arrays.asList("2", "3", "6", "7", "6"));
        JavaPairRDD<String, Integer> RDD4 = RDD3.mapToPair(s -> new Tuple2<String, Integer>(s, 2));
        JavaPairRDD<String, Tuple2<Optional<Integer>, Integer>> RDD5 = RDD2.rightOuterJoin(RDD4);
        System.out.println("rightOuterJoin：" + RDD5.collect());
    }

    @Test
    public void fullOuterJoin() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        JavaPairRDD<String, Integer> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
        JavaRDD<String> RDD3 = javaSparkContext.parallelize(Arrays.asList("2", "3", "6", "7", "6"));
        JavaPairRDD<String, Integer> RDD4 = RDD3.mapToPair(s -> new Tuple2<String, Integer>(s, 2));
        JavaPairRDD<String, Tuple2<Optional<Integer>, Optional<Integer>>> RDD5 = RDD2.fullOuterJoin(RDD4);
        System.out.println("fullOuterJoin：" + RDD5.collect());
    }

    @Test
    public void persisit() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = javaSparkContext.textFile("C:\\local\\es-cluster.log");
//        rdd.cache();
        rdd.persist(StorageLevel.MEMORY_ONLY());
        long startTime = System.currentTimeMillis();
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
        sorted.saveAsTextFile("C:\\Users\\liukai\\Desktop\\90\\baseInfo2.log");
        long oneTime = System.currentTimeMillis();
        System.out.println("第一次处理耗时：" + (oneTime - startTime));

        //获取所有字（空格分隔的字）
        JavaRDD<String> words2 = rdd.flatMap(e -> Arrays.asList(e.split(" ")).iterator());
        //将RDD的数据组装成对
        JavaPairRDD<String, Integer> wordAndOne2 = words.mapToPair(e -> new Tuple2<>(e, 1));
        //将成对的数据进行聚合
        JavaPairRDD<String, Integer> wordReduced2 = wordAndOne.reduceByKey((integer, integer2) -> integer + integer2);
        //java只支持按照key进行排序,我们先将Paer中kv呼唤
        JavaPairRDD<Integer, String> swaped2 = wordReduced.mapToPair(stringIntegerTuple2 -> stringIntegerTuple2.swap());
        //使用key排序
        JavaPairRDD<Integer, String> sorted2 = swaped.sortByKey();
        sorted.saveAsTextFile("C:\\Users\\liukai\\Desktop\\90\\baseInfo22.log");
        long twoTime = System.currentTimeMillis();
        System.out.println("第二次处理耗时：" + (twoTime - oneTime));
        javaSparkContext.stop();
    }

    @Test
    public void checkpoint() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = javaSparkContext.textFile("C:\\local\\es-cluster.log");
        javaSparkContext.setCheckpointDir("hdfs://node1.hadoop.com:8020/spark/checkpoint/");
        rdd.checkpoint();
        long startTime = System.currentTimeMillis();
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
        sorted.saveAsTextFile("C:\\Users\\liukai\\Desktop\\90\\baseInfo2.log");
        long oneTime = System.currentTimeMillis();
        System.out.println("第一次处理耗时：" + (oneTime - startTime));

        //获取所有字（空格分隔的字）
        JavaRDD<String> words2 = rdd.flatMap(e -> Arrays.asList(e.split(" ")).iterator());
        //将RDD的数据组装成对
        JavaPairRDD<String, Integer> wordAndOne2 = words.mapToPair(e -> new Tuple2<>(e, 1));
        //将成对的数据进行聚合
        JavaPairRDD<String, Integer> wordReduced2 = wordAndOne.reduceByKey((integer, integer2) -> integer + integer2);
        //java只支持按照key进行排序,我们先将Paer中kv呼唤
        JavaPairRDD<Integer, String> swaped2 = wordReduced.mapToPair(stringIntegerTuple2 -> stringIntegerTuple2.swap());
        //使用key排序
        JavaPairRDD<Integer, String> sorted2 = swaped.sortByKey();
        sorted.saveAsTextFile("C:\\Users\\liukai\\Desktop\\90\\baseInfo22.log");
        long twoTime = System.currentTimeMillis();
        System.out.println("第二次处理耗时：" + (twoTime - oneTime));
        javaSparkContext.stop();
    }

    @Test
    public void first() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        JavaPairRDD<String, Integer> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
        System.out.println("first,RDD1(JavaRDD)：" + RDD1.first());
        System.out.println("first,RDD3(JavaRairRDD)：" + RDD2.first());
    }

    @Test
    public void count() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        System.out.println("总数：" + RDD1.count());
    }

    @Test
    public void reduce() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        String reducedResult = RDD1.reduce((e1, e2) -> e1 + "_" + e2);
        System.out.println("reduced：" + reducedResult);
    }


    @Test
    public void collect() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        List<String> rddList = RDD1.collect();
        System.out.println("collect：" + rddList);
    }

    @Test
    public void take() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        List<String> rddList = RDD1.take(2);
        System.out.println("take：" + rddList);
    }

    @Test
    public void top() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        List<String> rddList = RDD1.top(2);
        System.out.println("top：" + rddList);
    }

    @Test
    public void takeOrdered() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        List<String> rddList = RDD1.takeOrdered(2);
        System.out.println("takeOrdered：" + rddList);
    }

    @Test
    public void aggregate() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        Integer reducedResult = RDD1.aggregate(2, (e1, e2) -> e1 + Integer.parseInt(e2), (e1, e2) -> e1 + e2);
        System.out.println("aggregate：" + reducedResult);
    }

    @Test
    public void fold() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        String reducedResult = RDD1.fold("加", (e1, e2) -> e1 + "_" + e2);
        System.out.println("fold：" + reducedResult);
    }

    @Test
    public void lookup() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        JavaPairRDD<String, Integer> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, Integer>(s, Integer.parseInt(s)));
        List<Integer> list = RDD2.lookup("4");
        System.out.println("lookup：" + list);
    }

    @Test
    public void countByValue() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        JavaPairRDD<String, Integer> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, Integer>(s, Integer.parseInt(s)));
        Map<Tuple2<String, Integer>, Long> map = RDD2.countByValue();
        System.out.println("countByValue：" + map);
    }

    @Test
    public void foreach() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        RDD1.foreach(s1 -> System.out.println("foreach：" + s1));
    }

    @Test
    public void foreachPartition() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        RDD1.foreachPartition(s1 -> s1.forEachRemaining(e -> System.out.println("foreachPartition：" + e)));
    }

    @Test
    public void sortBy() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        JavaRDD<String> RDD2 = RDD1.sortBy(x -> x, false, 1);
        System.out.println("sortBy：" + RDD2.collect());
    }

    @Test
    public void collectAsMap() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        JavaPairRDD<String, Integer> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, Integer>(s, Integer.parseInt(s)));
        Map<String, Integer> map = RDD2.collectAsMap();
        System.out.println("collectAsMap：" + map);
    }

}
