package com.owp.rdddemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * @描述:
 * @公司:
 * @作者:
 * @版本: 1.0.0
 * @日期: 2019-05-27 09:12:11
 */
public class Top {
    @Test
    public void top() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        List<String> rddList = RDD1.top(2);
        System.out.println("top：" + rddList);
    }
}
