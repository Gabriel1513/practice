package com.imooc.graph

import org.apache.spark
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.control._
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object deepwalk_test {
  def main(args: Array[String]): Unit ={
    println("Start")

    //Load Context
    val conf = new SparkConf().setAppName("movie_test").setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder()
      .appName("Spark Sql basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    //Load File
    val file_A = "file:///Users/gabriela/Documents/project/movie/ml-1m/ratings.dat"
    val file_B = "/Users/gabriela/Documents/project/movie/ml-1m/ratings.dat"
    val ratingsRdd = sc.textFile(file_B)

    //view Data
    ratingsRdd.top(5).foreach(println)

    //Load DF
    import spark.implicits._

    def data_prep(): RDD[Seq[String]] = {

      val ratingsDF = sc.textFile("/Users/gabriela/Documents/project/movie/ml-1m/ratings.dat").map(_.split("::")).map(x => (x(0),x(1),x(2),x(3))).toDF("userId","movieId","rating","timestamp")

      //过滤用户标准待设定
      //可以根据评分标准来过滤用户条数
      //其他

      // 按用户id分组，按timestamp排序，将movieId生成序列

      val df1 = ratingsDF.orderBy("userId","timestamp")

      val df3 = df1.groupBy("userId").agg(collect_list("movieId") as "movieIds").orderBy("userId")

      val df4 = df1.groupBy("userId").agg(concat_ws(" ", collect_list("movieId")) as "movieIds").orderBy("userId")

      println("df3 section:")
      df3.show()

      println("df4 section:")
      df4.show()

      // Convert to sequence

      val samples = df4.select("movieIds").rdd.map(r => r.getAs[String]("movieIds").split(" ").toSeq)

      return samples
    }

    val samples = data_prep()


    // Access each element pair and calculate statistics

    val pairSamples = samples.flatMap[String]( sample => {
      var pairSeq = Seq[String]()
      var previousItem:String = null
      sample.foreach((element:String) => {
        if(previousItem != null){
          pairSeq = pairSeq :+ (previousItem + ":" + element
            ) }
        previousItem = element
      })
      pairSeq
    })

    val pairCount = pairSamples.countByValue()

    // Create MoviePair and Transition Matrix
    val transferMatrix = scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, Long]]()
    val itemCount = scala.collection.mutable.Map[String, Long]()

    val log_num = sc.longAccumulator("t1")

    pairCount.foreach( pair =>{
      val pairItems = pair._1.split(":")
      val pairCount = pair._2
      log_num.add(1)
      println(log_num, pair._1)

      if (pairItems.length == 2){
        val item1 = pairItems.apply(0)
        val item2 = pairItems.apply(1)
        if (!transferMatrix.contains(item1)){
          transferMatrix(item1) = scala.collection.mutable.Map[String,Long]()
        }
        transferMatrix(item1)(item2) = pairCount
        itemCount(item1) = itemCount.getOrElse[Long](item1, 0) + pairCount
      }
    })

    // Random walk
    // Transfer Matrix
    // itemCount
    // select src element, get item distribution
    // select next element according to random sampling method
    // complete the process until path length satisfied

    def one_walk(transferMatrix : scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, Long]], itemCount : scala.collection.mutable.Map[String, Long], itemTotalCount:Long, sampleElement:String, sampleLength:Int): Seq[String] ={
      val sample = new Array[String](sampleLength)

      val randomDouble = Random.nextDouble()
      val firstElement = sampleElement

      sample(0) = firstElement

      var curElement = firstElement

      val loop = new Breaks;

      loop.breakable{
        for (i <- 1 until sampleLength){
          if (! transferMatrix.contains(curElement) || !itemCount.contains(curElement)){
            loop.break;
          }
          val nb_tmp = transferMatrix(curElement).keys.toArray
          val curNeighbors = sc.parallelize(nb_tmp)
          curElement = curNeighbors.takeSample(true,1).apply(0)
          sample(i) = curElement
        }
      }

      return sample.toSeq
    }

    var itemTotalCount: Long = 0

    for (x <- itemCount.keys.toList){
      itemTotalCount += itemCount(x)
    }

    var sampleElement = "590"
    var sampleLength = 5

    val walk = one_walk(transferMatrix, itemCount, itemTotalCount, sampleElement, sampleLength)

    val num_vertex: Long = itemCount.keys.toList.length
    val num_iter = 1


    def random_walk(num_iter: Int, sampleLength: Int, transferMatrix: scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, Long]], itemCount : scala.collection.mutable.Map[String, Long], itemTotalCount: Long): ArrayBuffer[Seq[String]] ={

      val samples = ArrayBuffer[Seq[String]]()
      for (i <- 1 until num_iter){
        for (sampleElement <- itemCount.keys.toList){
          var walk = one_walk(transferMatrix, itemCount, itemTotalCount, sampleElement, sampleLength)
          samples.append(walk)
        }
      }

      println("Complete")
      return samples
    }

    val walks = random_walk(num_iter, sampleLength, transferMatrix, itemCount, itemTotalCount)

    def word2vec_conversion(walks: ArrayBuffer[Seq[String]]): Map[String, Array[Float]]={
      val walks_data = sc.parallelize(walks)
      val word2vec = new Word2Vec().setMinCount(0).setVectorSize(50)
      val model = word2vec.fit(walks_data)
      val vectors = model.getVectors
      return vectors
    }

    val vectors_dict = word2vec_conversion(walks)

    sc.stop()
    println("Completion")
  }

}
