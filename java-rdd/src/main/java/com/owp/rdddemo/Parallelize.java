package com.owp.rdddemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @描述: 使用集合创建RDD
 * @公司:
 * @作者:
 * @版本: 1.0.0
 * @日期: 2019-05-26 23:39:05
 */
public class Parallelize {
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
        sorted.saveAsTextFile("C:\\Users\\liukai\\Desktop\\90\\baseInfo2.log");
        javaSparkContext.stop();
    }
}
