package com.owp.rdddemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

/**
 * @描述:
 * @公司:
 * @作者:
 * @版本: 1.0.0
 * @日期: 2019-05-27 09:02:39
 */
public class SortByKey {
    @Test
    public void sortByKey() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> RDD1 = javaSparkContext.parallelize(Arrays.asList("1", "2", "3", "4", "4", "6", "7", "8", "6"), 3);
        JavaPairRDD<String, Integer> RDD2 = RDD1.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
        class SecondaryComparator implements Comparator<String>, Serializable {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        }
        System.out.println("排序前分区数：" + RDD2.partitions().size());
        JavaPairRDD<String, Integer> RDD3 = RDD2.sortByKey(new SecondaryComparator(), true, 1);
        System.out.println("排序后分区数：" + RDD3.partitions().size());
        System.out.println("sortByKey：" + RDD3.collect());
    }
}
