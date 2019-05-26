package com.owp.rdd;

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
 * @日期: 2019-05-27 00:12:33
 */
public class MapPartitions {
    @Test
    public void mapPartitions() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9"), 3);
        JavaRDD<String> RDD2 = RDD1.mapPartitions(stringIterator -> {
            ArrayList<String> results = new ArrayList<>();
            while (stringIterator.hasNext()) {
                String item = stringIterator.next();
                results.add("处理过咯：" + item);
            }
            return results.iterator();
        });
        System.out.println("mapPartitions后：" + RDD2.collect());
    }
}
