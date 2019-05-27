package com.owp.rdddemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * @描述:
 * @公司:
 * @作者:
 * @版本: 1.0.0
 * @日期: 2019-05-26 23:58:11
 */
public class FlatMap {
    @Test
    public void flatMap() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<Integer> rdd = javaSparkContext.parallelize(Arrays.asList(2, 23, 12, 312, 312, 3, 123, 123, 14, 32, 54, 5, 123, 123, 235, 23, 51));
        JavaRDD<String> mapedRdd = rdd.flatMap(e -> {
            ArrayList<String> result = new ArrayList();
            if (e > 20) {
                result.add("字符：" + e);
            }
            return result.iterator();
        });
        mapedRdd.collect().forEach(e -> System.out.println(e));
    }
}
