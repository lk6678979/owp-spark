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
 * @日期: 2019-05-27 00:06:10
 */
public class Distinct {
    @Test
    public void distinct() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = javaSparkContext.parallelize(Arrays.asList("sssss", "sssss", "zzzz", "zzzz", "xyzzz"));
        JavaRDD<String> distinctRdd = rdd.distinct();
        distinctRdd.collect().forEach(e -> System.out.println("去重后:" + e));
    }
}
