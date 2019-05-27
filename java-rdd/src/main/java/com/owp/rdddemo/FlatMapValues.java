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
 * @作者: 刘恺
 * @版本: 1.0.0
 * @日期: 2019-05-27 09:00:59
 */
public class FlatMapValues {
    @Test
    public void flatMapValues() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9"), 3);
        JavaPairRDD<String, String> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, String>(s, s));
        JavaPairRDD<String, String> RDD3 = RDD2.flatMapValues(s -> Arrays.asList(s, s + "1", s + 2));
        System.out.println("flatMapValues后：" + RDD3.collect());
    }
}
