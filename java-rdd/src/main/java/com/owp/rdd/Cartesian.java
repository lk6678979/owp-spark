package com.owp.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.util.Arrays;

/**
 * @描述:
 * @公司:
 * @作者: 
 * @版本: 1.0.0
 * @日期: 2019-05-27 00:09:01
 */
public class Cartesian {
    @Test
    public void cartesian() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3"));
        JavaRDD<String> RDD2 = javaSparkContext.parallelize(Arrays.asList("a", "b", "c"));
        JavaPairRDD<String, String> cartesian = RDD1.cartesian(RDD2);
        cartesian.collect().forEach(e -> System.out.print("笛卡儿积:" + e + ","));
    }
}
