# Partitions对象
## 1. RDD分区对象（Partitions）
* 源码API
```
package org.apache.spark
trait Partition extends scala.AnyRef with scala.Serializable {
  def index : scala.Int
  override def hashCode() : scala.Int = { /* compiled code */ }
  override def equals(other : scala.Any) : scala.Boolean = { /* compiled code */ }
}
```
## 2. Hadoop分区对象
* 源码API
```
package org.apache.spark.rdd
private[spark] class HadoopPartition(rddId : scala.Int, override val index : scala.Int, s : org.apache.hadoop.mapred.InputSplit) extends scala.AnyRef with org.apache.spark.Partition {
  //返回partition的分片对象
  val inputSplit : org.apache.spark.SerializableWritable[org.apache.hadoop.mapred.InputSplit] = { /* compiled code */ }
  override def hashCode() : scala.Int = { /* compiled code */ }
  override def equals(other : scala.Any) : scala.Boolean = { /* compiled code */ }
  //返回一个包含环境变量和相应值的映射
  def getPipeEnvVars() : scala.collection.immutable.Map[scala.Predef.String, scala.Predef.String] = { /* compiled code */ }
}
```
## 3. 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/Partitions.java) 
```java
SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
JavaRDD<String> rdd = javaSparkContext.textFile("C:\\local\\es-cluster.log");
List<Partition> partitions = rdd.partitions();
//partition的数量
long size = partitions.size();
System.out.println("partitions数量："+size);
//遍历分区
for(Partition partition : partitions){
    //Partition父类方法：
    int index = partition.index();
    System.out.println("partitions下标："+ index);
    //返回一个包含环境变量和相应值的映射
    Map<String,String> pipeEnvVars = ((HadoopPartition) partition).getPipeEnvVars();
    System.out.println("partition的环境变量："+pipeEnvVars);
    //返回partition的分片对象
    SerializableWritable<org.apache.hadoop.mapred.InputSplit> inputSplit = ((HadoopPartition) partition).inputSplit();
    System.out.println("partition的分配对象："+inputSplit);
}
```
## 4. RDD分区函数（Partitioner）
` Partitioner是shuffle过程中key重分区时的策略，即计算key决定k-v属于哪个分区，Transformation是宽依赖的算子时，父RDD和子RDD之间会进行shuffle操作，shuffle涉及到网络开销，由于父RDD和子RDD中的partition是多对多关系，所以容易造成partition中数据分配不均匀，导致数据的倾斜
每个RDD都会有一个用来做分区计算的函数对象Partitioner,使用方法rdd.partitioner()可以获取 `
* 源码API
```
//获取分区函数
def partitioner : org.apache.spark.api.java.Optional[org.apache.spark.Partitioner] = { /* compiled code */ }
```
`在Spark默认提供两种划分器：哈希分区划分器(HashPartitioner)和范围划分奇(RangePartition)，且Partitioner只存在于(K,V)类型的RDD中，非(K,V)类型的Partitioner为None`
#### Partitioner类：
* 源码API
```
package org.apache.spark
abstract class Partitioner() extends scala.AnyRef with scala.Serializable {
  def numPartitions : scala.Int
  def getPartition(key : scala.Any) : scala.Int
}
//伴生类
object Partitioner extends scala.AnyRef with scala.Serializable {
  def defaultPartitioner(rdd : org.apache.spark.rdd.RDD[_], others : org.apache.spark.rdd.RDD[_]*) : org.apache.spark.Partitioner = { /* compiled code */ }
}
```
* 抽象类Partitioner定义了两个抽象方法numPartitions和getPartition。getPartition方法根据 输入的k-v对的key值返回一个Int型数据。
* 该抽象类伴生了一个Partitioner对象如下，主要包含defaultPartitioner函数，该函数定义了Partitioner的默认选择策略。如果设置了spark.default.parallelism，则使用该值作为默认partitions，否则使用上游RDD中partitions最大的数作为默认partitions。过滤出上游RDD中包含partitioner的RDD，选择包含有最大partitions并且isEligible的RDD，将该RDD中的partitioner设置为分区策略，否则返回一个带有默认partitions数的HashPartitioner作为Partitioner。Partition个数应该和partition个数最多的上游RDD一致，不然可能会导致OOM异常。
