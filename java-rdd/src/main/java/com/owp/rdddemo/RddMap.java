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
 * @日期: 2019-05-27 00:04:01
 */
public class RddMap {
    @Test
    public void map() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = javaSparkContext.parallelize(Arrays.asList("klhk lsad has", "dfsdf sdf", "sdgg hgfh", "yu yds f", "cxvx cvasd"));
        JavaRDD<String> mapedRdd = rdd.map(e -> e + "_我被map处理过");
        mapedRdd.collect().forEach(e -> System.out.println(e));
    }
}
