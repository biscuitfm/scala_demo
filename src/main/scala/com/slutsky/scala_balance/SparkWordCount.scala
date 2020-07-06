package com.slutsky.scala_balance

import com.slutksy.logging.Logger.info
import com.slutksy.logging.{MyClass, MyClass2, MyFunctions}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

//import org.apache.spark.api.java.JavaSparkContext

/**
 *
 * public interface Iterator<E> {
 *   boolean hasNext();
 *   E next();
 * }
 *
 *
 * public interface Iterable<T> {
 *   Iterator<T> iterator();
 * }
 *
 * Iterator是惰性序列
 *
 * map()函数接收两个参数，一个是函数，一个是Iterable，结果是Iterator transform过程
 *
 *
 * reduce的用法：reduce把一个函数作用在一个序列[x1, x2, x3, ...]上，这个函数必须接收两个参数，reduce把结果继续和序列的下一个元素做累积计算，其效果就是：
 *
 * reduce(f, [x1, x2, x3, x4]) = f(f(f(x1, x2), x3), x4)
 *
 */

object Balance {

  private val remote_file = "hdfs://Master:8020/shumengke/hadoop/dir1/mptest.info"

  def main(args:Array[String]): Unit = {
    val spark = SparkSession.builder()
//      .master("local") // 本机测试
      .master("yarn") // 集群运行
      .appName("Balance")
      .getOrCreate();

    println("First App")
    println("App Name :" + spark.sparkContext.appName)
    println("Deploy Mode :" + spark.sparkContext.deployMode)
    println("Master :" + spark.sparkContext.master)

/*    val sparkContext = spark.sparkContext
    val textFile = sparkContext.textFile("E:\\I17\\scala_demo\\README.md")

    textFile.foreach(println) // 单机模式正常
    val action_count = textFile.count() // action 操作 count
    println(action_count)
    val action_first = textFile.first() // action 操作 first
    println(action_first)
    val transform_filter = textFile.filter(line => line.contains("```")) // transform 操作 filter 产生新的 Dataset
    println(transform_filter.count())

    val transform_map = textFile.map(line => line.split(" ").size) // transform 操作 map 产生新的 Dataset
    println(transform_map.count())

    transform_map.foreach(println) // Iterator // 单机模式正常

    val action_reduce = transform_map.reduce((a, b) => Math.max(a, b)) // action 操作 reduce
    println(action_reduce)

    val transform_flatMap = textFile.flatMap(line => line.split(" ")) // transform 操作 flatMap 产生新的 Dataset

    val cached = transform_flatMap.cache() // 执行缓存操作 位于action之前

    transform_flatMap.foreach(println) // 单机模式正常

    println(transform_flatMap.count()) // action 操作 count



    println(cached.count())

    println(cached.count())

//    spark.stop()



    println("== == == == == WordCount Finish == == == == ==")

    val data = Array(1, 2, 3, 4, 5)

    val distData = sparkContext.parallelize(data)

    distData.foreach(println) // 单机模式正常

    println(distData.reduce((a,b) => a+b))

    val demoFile = sparkContext.textFile("E:\\I17\\scala_demo\\*.txt")

    val lineCount = demoFile.map(s=>s.length)
    lineCount.foreach(println) // 单机模式正常


    val whole = sparkContext.wholeTextFiles("E:\\I17\\scala_demo\\*.py")
    whole.foreach(println) // 单机模式正常

    val myFunMap = demoFile.map(MyFunctions.func1)
    println("doing myFunction")
    myFunMap.foreach(println) // 单机模式正常


    val slutskyFile = sparkContext.textFile("E:\\I17\\scala_demo\\slutsky.txt")

    slutskyFile.cache()

    val myClass = new MyClass
    val mapMyClass = myClass.doStuff(slutskyFile)
    mapMyClass.foreach(println) // 单机模式正常

    val myClass2 = new MyClass2
    val mapMyClass2 = myClass2.doStuff(slutskyFile)
    mapMyClass2.foreach(println) // 单机模式正常

    var counter = 0
    var rddArray = sparkContext.parallelize(data)

    /**
     *
     * To print all elements on the driver, one can use the collect() method to first bring the RDD to the driver node thus: rdd.collect().foreach(println).
     * This can cause the driver to run out of memory, though, because collect() fetches the entire RDD to a single machine;
     * if you only need to print a few elements of the RDD, a safer approach is to use the take(): rdd.take(100).foreach(println).
     *
     */

    rddArray.foreach(x => counter += x) // 单机模式正常 cluster模式只打印executor不打印driver

    println("begin collect println")
    rddArray.collect().foreach(println)

    println("begin take(3) println")
    rddArray.take(3).foreach(println)


    println("Error Counter value : " + counter)

    val accum = sparkContext.longAccumulator("sum accumulator")
    rddArray.foreach(x => accum.add(x))
    println("Correct Counter value : " + accum.value)


    val pair_lines = sparkContext.textFile("E:\\I17\\scala_demo\\pair.txt")
    val pair_origin = pair_lines.map(line => line)
    val pair_filter = pair_lines.filter(line => line.contains("Shu"))
    val pair_filter_length = pair_lines.filter(line => line.length>=4)

//    val pair_flatMap = pair_lines.flatMap(line => )
    pair_filter.collect().foreach(println)
    println("begin length>=4")
    pair_filter_length.collect().foreach(println)
    println("finish length>=4")
    pair_origin.collect().foreach(println)
    val pairs = pair_lines.map(s => (s, 1))
    pairs.collect().foreach(println)
    val pairs_count = pairs.reduceByKey((a,b) => a+b)
    pairs_count.collect().foreach(println)

    val union1 = sparkContext.textFile("E:\\I17\\scala_demo\\union_1.txt")
    val union2 = sparkContext.textFile("E:\\I17\\scala_demo\\union_2.txt")

    val after_union = union1.union(union2)

    val distinctData = union1.distinct()

    val intersection = union1.intersection(union2)

    after_union.collect().foreach(println)
    println("before intersection")
    intersection.collect().foreach(println)
    println("after intersection")
    distinctData.collect().foreach(println)

    val numArray = Array((1, 6, 3), (2, 3, 3), (1, 1, 2), (1, 3, 5), (2, 1, 2))
    val rddNum = sparkContext.parallelize(numArray)
    val tuRdd = rddNum.map(e => ((e._1, e._3), e))
    val sortTu = tuRdd.sortByKey()
    sortTu.collect().foreach(println)

    val numArrayNew = Array((1, 6, 3), (2, 3, 4), (1, 1, 5), (1, 3, 6), (2, 1, 7))
    val rddNumNew = sparkContext.parallelize(numArrayNew)
    val tuRddNew = rddNumNew.map(e => ((e._1, e._3), e))
    val joinTrans = tuRdd.join(tuRddNew)
    joinTrans.collect().foreach(println)
    //joinTrans.saveAsTextFile("E:\\I17\\scala_demo\\save_file")

    val broadcastVar = sparkContext.broadcast(Array(1, 2, 3))
    println(broadcastVar.value.toString)
    val df = spark.read.json("E:\\I17\\scala_demo\\people.json")
    df.show()
    df.printSchema()
//   (Scala-specific) Implicit methods available in Scala for converting common Scala objects into DataFrames.
    df.groupBy("age").count().show()
    df.groupBy("age", "name").count().show()
    import spark.implicits._
    df.select($"name", $"age" + 1).show()
    df.select($"age">20).show()
    df.groupBy($"name", $"age").count().show()

    df.createOrReplaceTempView("people")
    df.createGlobalTempView("people")
    val viewDF = spark.sql("select * from people")
    val globalViewDF = spark.sql("select * from global_temp.people").show()
    viewDF.show()
    spark.newSession.sql("select * from global_temp.people").show()


    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()

    val primitivesDS = Seq(1, 2, 3).toDS()
    val primitiveArray = primitivesDS.map(_ + 1).collect()
    primitiveArray.foreach(println)
    // mkString 分隔
    val path = "E:\\I17\\scala_demo\\people.json"

    val newPeopleDS = spark.read.json(path).as[Person]
    newPeopleDS.show()

    val mapPeopleDF = sparkContext.textFile("E:\\I17\\scala_demo\\people.txt")
      .map(_.split(" "))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    mapPeopleDF.show()
    mapPeopleDF.createOrReplaceTempView("mapPeopleDF")

    val teenagersDF = spark.sql("select name, age from mapPeopleDF where age between 10 and 20")
    teenagersDF.show()
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()



    val usersDF = spark.read.format("json").load("E:\\I17\\scala_demo\\people.json")
    usersDF.select("name", "age").show()
//    write.format("parquet").save("namesAndAges.parquet")
 */
    spark.close()
  }

  case class Person(name: String, age: Long)
}
