package com.owp.rdddemo;

import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.SerializableWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.rdd.HadoopPartition;
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
//        JavaRDD<String> rdd = javaSparkContext.parallelize(datas);
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

    @Test
    //使用集合创建PairRDD
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
//        JavaPairRDD<String, Integer> wordAndOne = javaSparkContext.parallelizePairs(tuples);
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


    @Test
    //textFile使用外部存储创建PairRDD
    public void textFile() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
//        JavaRDD<String> rdd = javaSparkContext.textFile("C:\\local\\es-*");
        JavaRDD<String> rdd = javaSparkContext.textFile("C:\\Users\\xxx\\Desktop\\90\\111\\222", 2);
//        JavaRDD<String> rdd = javaSparkContext.textFile("C:\\local\\es-*,C:\\local\\zs-*");
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

    @Test
    //wholeTextFile使用外部存储创建PairRDD
    public void wholeTextFile() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
//        JavaPairRDD<String,String> rdd = javaSparkContext.wholeTextFiles("C:\\Users\\xxx\\Desktop\\90\\111");
        JavaPairRDD<String, String> rdd = javaSparkContext.wholeTextFiles("C:\\Users\\xxx\\Desktop\\90\\111", 2);
        rdd.saveAsTextFile("C:\\Users\\xxx\\Desktop\\90\\111\\lkk.txt");
        javaSparkContext.stop();
    }


    @Test
    //操作rdd的partition
    public void partitions() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = javaSparkContext.textFile("C:\\local\\es-cluster.log");
        //获取分区函数对象
        Optional<Partitioner> partitioner = rdd.partitioner();
        List<Partition> partitions = rdd.partitions();
        //partition的数量
        long size = partitions.size();
        System.out.println("partitions数量：" + size);
        //遍历分区
        for (Partition partition : partitions) {
            //Partition父类方法：
            int index = partition.index();
            System.out.println("partitions下标：" + index);
            //返回一个包含环境变量和相应值的映射
            scala.collection.immutable.Map<String, String> pipeEnvVars = ((HadoopPartition) partition).getPipeEnvVars();
            System.out.println("partition的环境变量：" + pipeEnvVars);
            //返回partition的分片对象
            SerializableWritable<org.apache.hadoop.mapred.InputSplit> inputSplit = ((HadoopPartition) partition).inputSplit();
            System.out.println("partition的分配对象：" + inputSplit);
        }
    }

    @Test
    public void map() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = javaSparkContext.parallelize(Arrays.asList("klhk lsad has", "dfsdf sdf", "sdgg hgfh", "yu yds f", "cxvx cvasd"));
        JavaRDD<String> mapedRdd = rdd.map(e -> e + "_我被map处理过");
        mapedRdd.collect().forEach(e -> System.out.println(e));
    }

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

    @Test
    public void filter() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = javaSparkContext.parallelize(Arrays.asList("klhk lsad has", "sdfsdf sdf", "sdgg hgfh", "yu yds f", "cxvx cvasd"));
        JavaRDD<String> filterRdd = rdd.filter(e -> e.startsWith("s"));
        filterRdd.collect().forEach(e -> System.out.println("s开头才算哦:" + e));
    }

    @Test
    public void distinct() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = javaSparkContext.parallelize(Arrays.asList("sssss", "sssss", "zzzz", "zzzz", "xyzzz"));
        JavaRDD<String> distinctRdd = rdd.distinct();
        distinctRdd.collect().forEach(e -> System.out.println("去重后:" + e));
    }

    @Test
    public void union() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("aa", "aa", "bb", "cc", "dd"));
        JavaRDD<String> RDD2 = javaSparkContext.parallelize(Arrays.asList("aa", "dd", "ff"));
        JavaRDD<String> unionRDD = RDD1.union(RDD2);
        unionRDD.collect().forEach(e -> System.out.print("合并了:" + e + ","));
    }

    @Test
    public void intersection() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("aa", "aa", "bb", "cc", "dd"));
        JavaRDD<String> RDD2 = javaSparkContext.parallelize(Arrays.asList("aa", "dd", "ff"));
        JavaRDD<String> intersectionRDD = RDD1.intersection(RDD2);
        intersectionRDD.collect().forEach(e -> System.out.print("交集去重:" + e + ","));
    }

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

    @Test
    public void subtract() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("aa", "aa", "bb", "bb", "cc", "dd"));
        JavaRDD<String> RDD2 = javaSparkContext.parallelize(Arrays.asList("aa", "dd", "ff"));
        JavaRDD<String> subtractRDD = RDD1.subtract(RDD2);
        subtractRDD.collect().forEach(e -> System.out.print("RDD1有，RDD2没有:" + e + ","));
    }

    @Test
    public void cartesian() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3"));
        JavaRDD<String> RDD2 = javaSparkContext.parallelize(Arrays.asList("a", "b", "c"));
        JavaPairRDD<String, String> cartesian = RDD1.cartesian(RDD2);
        cartesian.collect().forEach(e -> System.out.print("笛卡儿积:" + e + ","));
    }

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

    @Test
    public void glom() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9"), 3);
        JavaRDD<List<String>> rddItem = RDD1.glom();
        System.out.println("glom后：" + rddItem.collect());
    }

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

    @Test
    public void mapToPail() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9"), 3);
        JavaPairRDD<String, String> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, String>(s, s));
        System.out.println("mapToPail后：" + RDD2.collect());
    }

    @Test
    public void zip() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9"), 3);
        JavaRDD<Integer> RDD2 = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9), 3);
        JavaPairRDD rdds = RDD1.zip(RDD2);
        System.out.println("zip后：" + rdds.collect());
    }

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
        sorted.saveAsTextFile("C:\\Users\\xxx\\Desktop\\90\\baseInfo2.log");
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
        sorted.saveAsTextFile("C:\\Users\\xxx\\Desktop\\90\\baseInfo22.log");
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
        sorted.saveAsTextFile("C:\\Users\\xxx\\Desktop\\90\\baseInfo2.log");
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
        sorted.saveAsTextFile("C:\\Users\\xxx\\Desktop\\90\\baseInfo22.log");
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
