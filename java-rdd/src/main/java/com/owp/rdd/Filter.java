package com.owp.rdd;

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
 * @日期: 2019-05-27 00:05:41
 */
public class Filter {
    @Test
    public void filter() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = javaSparkContext.parallelize(Arrays.asList("klhk lsad has", "sdfsdf sdf", "sdgg hgfh", "yu yds f", "cxvx cvasd"));
        JavaRDD<String> filterRdd = rdd.filter(e -> e.startsWith("s"));
        filterRdd.collect().forEach(e -> System.out.println("s开头才算哦:" + e));
    }
}
