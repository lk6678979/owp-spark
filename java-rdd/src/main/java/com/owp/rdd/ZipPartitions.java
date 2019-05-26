package com.owp.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @描述:
 * @公司:
 * @作者:
 * @版本: 1.0.0
 * @日期: 2019-05-27 00:14:35
 */
public class ZipPartitions {
    @Test
    public void zipPartitions() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9"), 3);
        JavaRDD<Integer> RDD2 = javaSparkContext.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3);
        JavaRDD<String> zipPartitionsRDD = RDD1.zipPartitions(RDD2, (integerIterator, integerIterator2) -> {
            List<String> arrayList = new ArrayList<>();
            while (integerIterator.hasNext() && integerIterator2.hasNext())
                arrayList.add(integerIterator.next().toString() + "_" + integerIterator2.next().toString());
            return arrayList.iterator();
        });
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" + zipPartitionsRDD.collect());
    }
}
