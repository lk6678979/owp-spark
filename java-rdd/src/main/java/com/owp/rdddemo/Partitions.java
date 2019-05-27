package com.owp.rdddemo;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.spark.Partition;
import org.apache.spark.Partitioner;
import org.apache.spark.SerializableWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.rdd.HadoopPartition;
import org.junit.Test;

import java.util.List;

/**
 * @描述: 操作rdd的partition
 * @公司:
 * @作者:
 * @版本: 1.0.0
 * @日期: 2019-05-26 23:56:56
 */
public class Partitions {
    @Test
    public void partitions() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = javaSparkContext.textFile("C:\\local\\es-cluster.log");
        //获取分区函数对象
        Optional<Partitioner> partitioner = rdd.partitioner();
        List<Partition> partitions = rdd.partitions();
        //partition的数量
        long size = partitions.size();
        System.out.println("partitions数量：" + size);
        //遍历分区
        for (Partition partition : partitions) {
            //Partition父类方法：
            int index = partition.index();
            System.out.println("partitions下标：" + index);
            //返回一个包含环境变量和相应值的映射
            scala.collection.immutable.Map<String, String> pipeEnvVars = ((HadoopPartition) partition).getPipeEnvVars();
            System.out.println("partition的环境变量：" + pipeEnvVars);
            //返回partition的分片对象
            SerializableWritable<InputSplit> inputSplit = ((HadoopPartition) partition).inputSplit();
            System.out.println("partition的分配对象：" + inputSplit);
        }
    }
}
