package alt

import org.apache.spark.{SparkConf, SparkContext}

object buildprojectsekai extends App {
  val conf = new SparkConf().setAppName("hI").setMaster("local");
  val sc =new SparkContext(conf)
  //ktra doc
//  val data = Array(1,2,3,4,5)
//  val rdd = sc.parallelize(data)
//  rdd.foreach(println)
//  sc.stop()

   // binh phuong :tao rdd tu rdd co san tu sparkcontext
//    val data = Seq(1,2,3,4,5)
//  val rdd = sc.parallelize(data)
//  val BinhPhuong_rdd = rdd.map(a=>a*a)
//  rdd.foreach(println)
//  BinhPhuong_rdd .foreach(println)
//  sc.stop()

  //  lay tu file
//  val filePath ="C:/retails.csv"
//  val rdd = sc.textFile(filePath)
//  rdd.foreach(println)

  // ham flatmap: lam phang doi tuong
//  val rdd = sc.parallelize(Array("Sech","ABC","AAAAAA","bantumlum"))
//  val FLATMAP_1 = rdd. flatMap(Line=>Line.split(""))
//  val FLATMAP_2 = rdd. flatMap(word=>word.toCharArray)
//  FLATMAP_1.foreach(println)
//  FLATMAP_2.foreach(println)

  // ham reduce: lay g/tri cuoi cung
//  val data = Array(1,2,3,4,5,6,7,8,9,10)
//  val rdd = sc.parallelize(data)
//  val Reduce_Rdd_min = rdd.reduce((a,b)=>a min b)
//  val Reduce_Rdd_max = rdd.reduce((a,b)=>a max b)
//  val Reduce_Rdd_sum = rdd.reduce((a,b)=>a + b)
//  println("Be nhat:"+Reduce_Rdd_min)
//  println("lon nhat:"+Reduce_Rdd_max)
//  println("Tong:"+Reduce_Rdd_sum)

  //reduce_by_key
//  val data = Array(("tao",3),("dua",2),("tao",5),("thit cho",2),("xuc xich",6))
//  val rdd = sc.parallelize(data)
//  val reduce_by_key_RDD = rdd.reduceByKey((x,y)=>x+y)
//  reduce_by_key_RDD.foreach(println)

  //sort-by
//  val data = Array(10,2,34,1,3,20,18,9,100)
//  val rdd = sc.parallelize(data)
//  val sort_RDD = rdd.sortBy(m=>m, ascending = false)
//  sort_RDD.foreach(println)

  //wordcount
//  val Map_rdd = sc.textFile("D:/data.txt").flatMap(x=>x.split(" ")).map(x=>(x,1))
//  val Reduce_rdd = Map_rdd.reduceByKey((x,y)=>x+y)
//  Map_rdd.foreach(println)
//  Reduce_rdd.foreach(println)

  //group by key
    val data = Array(("tao",3),("dua",2),("tao",5),("thit cho",2),("xuc xich",6))
    val rdd = sc.parallelize(data)
   val group_by_key_RDD = rdd.groupByKey()
  group_by_key_RDD.foreach(println)
  }


