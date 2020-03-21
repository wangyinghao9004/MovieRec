package com.atguigu.statistics

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, KeyValueGroupedDataset, SparkSession}

case class IdAndGenres(mid:String,genres:String)

object Test {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\java\\spark\\hadoop-eclipse-plugin")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Test").master("local[*]").getOrCreate()

    //所有的电影类别
    val genres = List("Action", "Adventure", "Animation", "Comedy", "Ccrime", "Documentary", "Drama", "Family", "Fantasy", "Foreign", "History", "Horror", "Music", "Mystery"
      , "Romance", "Science", "Tv", "Thriller", "War", "Western")
    //将电影类别转换成RDD
    val genresRDD = spark.sparkContext.makeRDD(genres)

    /**
      * 统计每种电影类型中评分最高的10个电影
      */

    import spark.implicits._
    val movieDF = spark.sparkContext
      .textFile("D:\\IDEA项目\\MovieRec\\recommender\\statisticRecommender\\src\\main\\scala\\com\\atguigu\\statistics\\movies.csv")
      .map(line => {
        val lines = line.split("\\^")
        Movie(lines(0).toInt, lines(1).trim, lines(2).trim, lines(3).trim, lines(4).trim, lines(5).trim, lines(6).trim, lines(7).trim, lines(8).trim, lines(9).trim)
      })
      .toDF

    val ratingDF=spark.sparkContext
      .textFile("D:\\IDEA项目\\MovieRec\\recommender\\statisticRecommender\\src\\main\\scala\\com\\atguigu\\statistics\\ratings.csv")
      .map(line=>{
        val arr=line.split(",")
        Rating(arr(0).toInt, arr(1).toInt, arr(2).toDouble, arr(3).toInt)
        }).toDF()

    ratingDF.createOrReplaceTempView("ratings")
    movieDF.createOrReplaceTempView("movies")
    //统计每个电影的平均评分
   val framea= spark.sql("select mid, avg(score) as avg from ratings group by mid").toDF()

    val frameg: DataFrame = spark.sql("select mid,genres from movies").flatMap(line => {
      val lines = line.getAs(1).toString.split("\\|")
      var str = ""
      for (i <- lines) {
        str = str + line.getAs(0) + "~" + i + ","
      }
      str.substring(0, str.length - 1).split(",")
    }).map(line => {
      val lines = line.split("~")
      IdAndGenres(lines(0), lines(1))
    }).toDF()

    import org.apache.spark.sql.functions._

    framea.join(frameg, Seq("mid", "mid"), "left")
      .rdd
      .map(line=>{
        (line.getAs[String]("genres"),(line.getAs[Int](0),line.getAs[Double]("avg")))
      })
      .groupByKey()
      .map{
        case (genres,items)=>GenresRecommendation(genres,items.toList.sortWith(_._2 > _._2).take(10).map(line=>Recommendation(line._1,line._2)))
      }.toDF().show()


  }
}
