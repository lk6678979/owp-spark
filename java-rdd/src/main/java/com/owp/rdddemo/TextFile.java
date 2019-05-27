package com.owp.rdddemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @描述: 使用外部文件存储创建PairRDD（路径可以是任意文件理解，例如本地文件、网络文件、hdfs文件等）
 * @公司:
 * @作者:
 * @版本: 1.0.0
 * @日期: 2019-05-26 23:53:42
 */
public class TextFile {
    @Test
    //textFile使用外部存储创建PairRDD
    public void textFile() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
//        JavaRDD<String> rdd = javaSparkContext.textFile("C:\\local\\es-*");
        JavaRDD<String> rdd = javaSparkContext.textFile("C:\\Users\\liukai\\Desktop\\90\\111\\222", 2);
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
        sorted.saveAsTextFile("C:\\Users\\liukai\\Desktop\\90\\baseInfo2.log");
        javaSparkContext.stop();
    }
}
