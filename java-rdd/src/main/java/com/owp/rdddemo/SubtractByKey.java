package com.owp.rdddemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @描述:
 * @公司:
 * @作者:
 * @版本: 1.0.0
 * @日期: 2019-05-27 09:03:34
 */
public class SubtractByKey {
    @Test
    public void subtractByKey() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        JavaPairRDD<String, Integer> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
        JavaRDD<String> RDD3 = javaSparkContext.parallelize(Arrays.asList("2", "3", "6", "7", "6"));
        JavaPairRDD<String, Integer> RDD4 = RDD3.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
        JavaPairRDD<String, Integer> RDD5 = RDD2.subtractByKey(RDD4);
        System.out.println("subtractByKey：" + RDD5.collect());
    }
}
