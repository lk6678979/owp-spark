package com.owp.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @描述: 用集合创建PairRDD
 * @公司:
 * @作者:
 * @版本: 1.0.0
 * @日期: 2019-05-26 23:51:30
 */
public class ParallelizePairs {
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
//        JavaPairRDD<String, Integer> wordAndOne = javaSparkContext.parallelizePairs(tuples);
        JavaPairRDD<String, Integer> wordAndOne = javaSparkContext.parallelizePairs(tuples, 3);
        //将成对的数据进行聚合
        JavaPairRDD<String, Integer> wordReduced = wordAndOne.reduceByKey((integer, integer2) -> integer + integer2);
        //java只支持按照key进行排序,我们先将Paer中kv呼唤
        JavaPairRDD<Integer, String> swaped = wordReduced.mapToPair(stringIntegerTuple2 -> stringIntegerTuple2.swap());
        //使用key排序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey();
        sorted.saveAsTextFile("C:\\Users\\liukai\\Desktop\\90\\baseInfo2.log");
        javaSparkContext.stop();
    }
}
