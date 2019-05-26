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
 * @日期: 2019-05-27 00:10:06
 */
public class RandomSplit {
    @Test
    public void randomSplit() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9"));
        JavaRDD<String>[] rdds = RDD1.randomSplit(new double[]{0.33, 0.22, 0.44});
        for (int i = 0; i < rdds.length; i++) {
            final int index = i;
            JavaRDD rddItem = rdds[i];
            rddItem.collect().forEach(e -> System.out.print("分片【" + index + "】:" + e + ","));
        }
    }
}
