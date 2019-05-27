package com.owp.rdddemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.util.Arrays;

/**
 * @描述:
 * @公司:
 * @作者:
 * @版本: 1.0.0
 * @日期: 2019-05-27 00:07:28
 */
public class Intersection {
    @Test
    public void intersection() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("aa", "aa", "bb", "cc", "dd"));
        JavaRDD<String> RDD2 = javaSparkContext.parallelize(Arrays.asList("aa", "dd", "ff"));
        JavaRDD<String> intersectionRDD = RDD1.intersection(RDD2);
        intersectionRDD.collect().forEach(e -> System.out.print("交集去重:" + e + ","));
    }
}
