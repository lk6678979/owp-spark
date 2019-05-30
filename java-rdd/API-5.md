## 1. cache和persist
* Spark可以将RDD持久化到内存或者磁盘文件系统中，把RDD持久化到内存中可以极大的提高迭代计算以及各计算模型之间的数据共享，一般情况下执行节点60%内存用于缓存数据，40%用于运行任务。
spark中使用persist和cache操作进行持久化，其中cache是persist()的特例
* 我们可以把需要多次计算的数据RDD换成起来，后面反复使用可以节省大量时间
* 源码API:
```scala
def cache() : org.apache.spark.api.java.JavaRDD[T] = { /* compiled code */ }
def persist(newLevel : org.apache.spark.storage.StorageLevel) : org.apache.spark.api.java.JavaRDD[T] = { /* compiled code */ }
```
### persist的Storagelevel可选类型：
```scala
val NONE : org.apache.spark.storage.StorageLevel = { /* compiled code */ }
val DISK_ONLY : org.apache.spark.storage.StorageLevel = { /* compiled code */ }
val DISK_ONLY_2 : org.apache.spark.storage.StorageLevel = { /* compiled code */ }
val MEMORY_ONLY : org.apache.spark.storage.StorageLevel = { /* compiled code */ }
val MEMORY_ONLY_2 : org.apache.spark.storage.StorageLevel = { /* compiled code */ }
val MEMORY_ONLY_SER : org.apache.spark.storage.StorageLevel = { /* compiled code */ }
val MEMORY_ONLY_SER_2 : org.apache.spark.storage.StorageLevel = { /* compiled code */ }
val MEMORY_AND_DISK : org.apache.spark.storage.StorageLevel = { /* compiled code */ }
val MEMORY_AND_DISK_2 : org.apache.spark.storage.StorageLevel = { /* compiled code */ }
val MEMORY_AND_DISK_SER : org.apache.spark.storage.StorageLevel = { /* compiled code */ }
val MEMORY_AND_DISK_SER_2 : org.apache.spark.storage.StorageLevel = { /* compiled code */ }
val OFF_HEAP : org.apache.spark.storage.StorageLevel = { /* compiled code */ }
```
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/Persisit.java) 
```java
 @Test
    public void persisit() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = javaSparkContext.textFile("C:\\local\\es-cluster.log");
//        rdd.cache();
        rdd.persist(StorageLevel.MEMORY_ONLY());
        long startTime = System.currentTimeMillis();
        //获取所有字（空格分隔的字）
        JavaRDD<String> words = rdd.flatMap(e -> Arrays.asList(e.split(" ")).iterator());
        //将RDD的数据组装成对
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(e -> new Tuple2<>(e, 1));
        //将成对的数据进行聚合
        JavaPairRDD<String, Integer> wordReduced = wordAndOne.reduceByKey((integer, integer2) -> integer + integer2);
        //java只支持按照key进行排序,我们先将Paer中kv呼唤
        JavaPairRDD<Integer, String> swaped = wordReduced.mapToPair(stringIntegerTuple2 -> stringIntegerTuple2.swap());
        //使用key排序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey();
        sorted.saveAsTextFile("C:\\Users\\xxx\\Desktop\\90\\baseInfo2.log");
        long oneTime = System.currentTimeMillis();
        System.out.println("第一次处理耗时：" + (oneTime - startTime));
        //获取所有字（空格分隔的字）
        JavaRDD<String> words2 = rdd.flatMap(e -> Arrays.asList(e.split(" ")).iterator());
        //将RDD的数据组装成对
        JavaPairRDD<String, Integer> wordAndOne2 = words.mapToPair(e -> new Tuple2<>(e, 1));
        //将成对的数据进行聚合
        JavaPairRDD<String, Integer> wordReduced2 = wordAndOne.reduceByKey((integer, integer2) -> integer + integer2);
        //java只支持按照key进行排序,我们先将Paer中kv呼唤
        JavaPairRDD<Integer, String> swaped2 = wordReduced.mapToPair(stringIntegerTuple2 -> stringIntegerTuple2.swap());
        //使用key排序
        JavaPairRDD<Integer, String> sorted2 = swaped.sortByKey();
        sorted.saveAsTextFile("C:\\Users\\xxx\\Desktop\\90\\baseInfo22.log");
        long twoTime = System.currentTimeMillis();
        System.out.println("第二次处理耗时：" + (twoTime - oneTime));
        javaSparkContext.stop();
    }
------------------------结果---------------------
第一次处理耗时：4655
第二次处理耗时：1264
```
## 3. checkpoint 设置检查点
* 1.Spark 在生产环境下经常会面临 Transformation 的 RDD 非常多(例如一个Job 中包含1万个RDD) 或者是具体的 Transformation 产生的 RDD 本身计算特别复杂和耗时(例如计算时常超过1个小时) , 可能业务比较复杂，此时我们必需考虑对计算结果的持久化。
* 2.Spark 是擅长多步骤迭代，同时擅长基于 Job 的复用。这个时候如果曾经可以对计算的过程进行复用，就可以极大的提升效率。因为有时候有共同的步骤，就可以免却重复计算的时间。
* 3.如果采用 persists 把数据在内存中的话，虽然最快速但是也是最不可靠的；如果放在磁盘上也不是完全可靠的，例如磁盘会损坏，系统管理员可能会清空磁盘。
* 4.Checkpoint 的产生就是为了相对而言更加可靠的持久化数据，在 Checkpoint 可以指定把数据放在本地并且是多副本的方式，但是在正常生产环境下放在 HDFS 上，这就天然的借助HDFS 高可靠的特征来完成最大化的可靠的持久化数据的方式。
* 5.Checkpoint 是为了最大程度保证绝对可靠的复用 RDD 计算数据的 Spark 的高级功能，通过 Checkpoint 我们通过把数据持久化到 HDFS 上来保证数据的最大程度的安任性
* 6.Checkpoint 就是针对整个RDD 计算链条中特别需要数据持久化的环节(后面会反覆使用当前环节的RDD) 开始基于HDFS 等的数据持久化复用策略，通过对 RDD 启动 Checkpoint 机制来实现容错和高可用；
* 7.rdd使用checkpoint之前，需要先使用JavaSparkContext.setCheckpointDir设置检查的数据存储路径，可以在本地也可以用HDFS
* ⭐注意：checkpoint本身如果数据太大也会很耗时
#### 编码测试，[前往JAVADEMO](https://github.com/lk6678979/owp-spark/blob/master/java-rdd/src/main/java/com/owp/rdddemo/Checkpoint.java) 
```java
 @Test
    public void checkpoint() {
        SparkConf sparkConf = new SparkConf().setAppName("demo").setMaster("local").set("spark.executor.memory", "1g");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = javaSparkContext.textFile("C:\\local\\es-cluster.log");
        javaSparkContext.setCheckpointDir("hdfs://node1.hadoop.com:8020/spark/checkpoint/");
        rdd.checkpoint();
        long startTime = System.currentTimeMillis();
        //获取所有字（空格分隔的字）
        JavaRDD<String> words = rdd.flatMap(e -> Arrays.asList(e.split(" ")).iterator());
        //将RDD的数据组装成对
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(e -> new Tuple2<>(e, 1));
        //将成对的数据进行聚合
        JavaPairRDD<String, Integer> wordReduced = wordAndOne.reduceByKey((integer, integer2) -> integer + integer2);
        //java只支持按照key进行排序,我们先将Paer中kv呼唤
        JavaPairRDD<Integer, String> swaped = wordReduced.mapToPair(stringIntegerTuple2 -> stringIntegerTuple2.swap());
        //使用key排序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey();
        sorted.saveAsTextFile("C:\\Users\\xxx\\Desktop\\90\\baseInfo2.log");
        long oneTime = System.currentTimeMillis();
        System.out.println("第一次处理耗时：" + (oneTime - startTime));

        //获取所有字（空格分隔的字）
        JavaRDD<String> words2 = rdd.flatMap(e -> Arrays.asList(e.split(" ")).iterator());
        //将RDD的数据组装成对
        JavaPairRDD<String, Integer> wordAndOne2 = words.mapToPair(e -> new Tuple2<>(e, 1));
        //将成对的数据进行聚合
        JavaPairRDD<String, Integer> wordReduced2 = wordAndOne.reduceByKey((integer, integer2) -> integer + integer2);
        //java只支持按照key进行排序,我们先将Paer中kv呼唤
        JavaPairRDD<Integer, String> swaped2 = wordReduced.mapToPair(stringIntegerTuple2 -> stringIntegerTuple2.swap());
        //使用key排序
        JavaPairRDD<Integer, String> sorted2 = swaped.sortByKey();
        sorted.saveAsTextFile("C:\\Users\\xxx\\Desktop\\90\\baseInfo22.log");
        long twoTime = System.currentTimeMillis();
        System.out.println("第二次处理耗时：" + (twoTime - oneTime));
        javaSparkContext.stop();
    }
-------------------结果--------------------------
第一次处理耗时：14846
第二次处理耗时：1268
```
