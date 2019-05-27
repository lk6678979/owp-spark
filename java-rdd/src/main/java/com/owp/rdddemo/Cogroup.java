package com.owp.rdddemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.Map;

/**
 * @描述:
 * @公司:
 * @作者:
 * @版本: 1.0.0
 * @日期: 2019-05-27 09:03:02
 */
public class Cogroup {
    @Test
    public void cogroup() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<Tuple2<String, Float>> scoreDetails1 = javaSparkContext.parallelize(Arrays.asList(new Tuple2("xiaoming", 75)
                , new Tuple2("xiaoming", 90)
                , new Tuple2("lihua", 95)
                , new Tuple2("lihua", 96)));
        JavaRDD<Tuple2<String, Float>> scoreDetails2 = javaSparkContext.parallelize(Arrays.asList(new Tuple2("xiaoming", 75)
                , new Tuple2("lihua", 60)
                , new Tuple2("lihua", 62)));
        JavaRDD<Tuple2<String, Float>> scoreDetails3 = javaSparkContext.parallelize(Arrays.asList(new Tuple2("xiaoming", 75)
                , new Tuple2("xiaoming", 45)
                , new Tuple2("lihua", 24)
                , new Tuple2("lihua", 57)));

        JavaPairRDD<String, Float> scoreMapRDD1 = JavaPairRDD.fromJavaRDD(scoreDetails1);
        JavaPairRDD<String, Float> scoreMapRDD2 = JavaPairRDD.fromJavaRDD(scoreDetails2);
        JavaPairRDD<String, Float> scoreMapRDD3 = JavaPairRDD.fromJavaRDD(scoreDetails2);

        JavaPairRDD<String, Tuple3<Iterable<Float>, Iterable<Float>, Iterable<Float>>> cogroupRDD = scoreMapRDD1.cogroup(scoreMapRDD2, scoreMapRDD3);


        Map<String, Tuple3<Iterable<Float>, Iterable<Float>, Iterable<Float>>> tuple3 = (Map<String, Tuple3<Iterable<Float>, Iterable<Float>, Iterable<Float>>>) cogroupRDD.collectAsMap();
        for (String key : tuple3.keySet()) {
            System.out.println("数据：(" + key + ", " + tuple3.get(key) + ")");
        }
//        System.out.println("sortByKey：" + RDD3.collect());
    }
}
