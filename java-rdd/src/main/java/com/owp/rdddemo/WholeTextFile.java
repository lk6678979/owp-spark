package com.owp.rdddemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

/**
 * @描述: 使用外部存储创建PairRDD
 * @公司:
 * @作者:
 * @版本: 1.0.0
 * @日期: 2019-05-26 23:55:40
 */
public class WholeTextFile {
    @Test
    public void wholeTextFile() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
//        JavaPairRDD<String,String> rdd = javaSparkContext.wholeTextFiles("C:\\Users\\xxx\\Desktop\\90\\111");
        JavaPairRDD<String, String> rdd = javaSparkContext.wholeTextFiles("C:\\Users\\xxx\\Desktop\\90\\111", 2);
        rdd.saveAsTextFile("C:\\Users\\xxx\\Desktop\\90\\111\\lkk.txt");
        javaSparkContext.stop();
    }
}
