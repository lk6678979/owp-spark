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
 * @日期: 2019-05-27 00:07:54
 */
public class CoalesceRepartition {
    @Test
    public void coalesceRepartition() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("aa", "aa", "bb", "cc", "dd"), 2);
        System.out.println("原分区：" + RDD1.partitions().size());
        JavaRDD<String> RDD2 = RDD1.coalesce(1);
        System.out.println("coalesce分区：" + RDD2.partitions().size());
        JavaRDD<String> RDD3 = RDD2.repartition(3);
        System.out.println("repartition分区：" + RDD3.partitions().size());
    }
}
