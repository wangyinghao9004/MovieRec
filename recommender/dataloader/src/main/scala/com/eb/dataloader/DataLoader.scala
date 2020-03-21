package com.eb.dataloader

import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

import scala.xml.XML

/**
  * movie数据集
  *
  * 电影id        1
  * 电影名称      abc
  * 电影描述      abc
  * 电影时长      138 minutes
  * 电影发行日期  1997
  * 电影拍摄日期  1995
  * 电影语言      English
  * 电影类型      a|b|c|...
  * 电影演员      a|b|c|...
  * 电影导演      a|b|c|...
  *
  * 电影的tag    tag1|tag2|tag3|...
  */

/**
  * rating用户对电影评分
  *
  * 用户id
  * 电影id
  * 用户对电影的评分
  * 用户对电影评分时间
  */

/**
  * tag用户对电影标签
  *
  * 用户id
  * 电影id
  * 标签具体内容
  * 用户对电影打标签时间
  */

case class Movie(mid: Int, name: String, descri: String, timeLong: String, issue: String
                 , shot: String, language: String, genres: String, actors: String, directors: String)

case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)

/**
  * mongodb连接配置
  *
  * @param uri uri
  * @param db  数据库
  */
case class MongoConfig(uri: String, db: String)

/**
  * ES连接配置
  *
  * @param httpHosts      http主机列表，以，分割
  * @param transportHosts TransPort主机列表
  * @param index          需要操作的索引
  * @param clustername    集群名称
  */
case class ESConfig(httpHosts: String, transportHosts: String, index: String, clustername: String)

/**
  * 数据主加载服务
  */
object DataLoader {

  //程序入口
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\java\\spark\\hadoop-eclipse-plugin")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val filePath = "D:\\IDEA项目\\MovieRec\\recommender\\dataloader\\src\\main\\scala\\com\\eb\\dataloader\\DataLoader.xml"
    val someXml = XML.loadFile(filePath)
    val mongo_uri = (someXml \\ "mongo.uri").text
    val mongo_db = (someXml \\ "mongo.db").text
    val es_httpHosts = (someXml \\ "es.httpHosts").text
    val es_transportHosts = (someXml \\ "es.transportHosts").text
    val es_index = (someXml \\ "es.index").text
    val es_cluster_name = (someXml \\ "es.cluster.name").text
    val MONGODB_MOVIE_COLLECTION = (someXml \\ "MONGODB_MOVIE_COLLECTION").text
    val MONGODB_RATING_COLLECTION = (someXml \\ "MONGODB_RATING_COLLECTION").text
    val MONGODB_TAG_COLLECTION = (someXml \\ "MONGODB_TAG_COLLECTION").text
    val ES_MOVIE_COLLECTION = (someXml \\ "ES_MOVIE_COLLECTION").text
    val ES_RATING_COLLECTION = (someXml \\ "ES_RATING_COLLECTION").text
    val ES_TAG_COLLECTION = (someXml \\ "ES_TAG_COLLECTION").text
    val MOVIE_DATA_PATH = (someXml \\ "MOVIE_DATA_PATH").text
    val RATING_DATA_PATH = (someXml \\ "RATING_DATA_PATH").text
    val TAG_DATA_PATH = (someXml \\ "TAG_DATA_PATH").text
    val ES_MOVIE_INDEX = "Movie"
    val spark = SparkSession.builder().appName("DataLoader").master("local[*]").getOrCreate()

    import spark.implicits._

    //加载数据movie,rating,tag加载进来
    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    val movieDF = movieRDD.map(line => {
      val arr = line.split("\\^")
      Movie(arr(0).toInt, arr(1).trim, arr(2).trim, arr(3).trim, arr(4).trim, arr(5).trim, arr(6).trim, arr(7).trim, arr(8).trim, arr(9).trim)
    }).toDF()

    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map(line => {
      val arr = line.split(",")
      Rating(arr(0).toInt, arr(1).toInt, arr(2).toDouble, arr(3).toInt)
    }).toDF()

    val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)
    val tagDF = tagRDD.map(line => {
      val arr = line.split(",")
      Tag(arr(0).toInt, arr(1).toInt, arr(2).trim, arr(3).toInt)
    }).toDF()

    implicit val mongoConfig: MongoConfig = MongoConfig(mongo_uri, mongo_db)
    //将数据保存在mongodb
    storeDataInMongoDB(movieDF, ratingDF, tagDF, MONGODB_MOVIE_COLLECTION, MONGODB_RATING_COLLECTION, MONGODB_TAG_COLLECTION, movieDF, ratingDF, tagDF)

    /**
      * movie数据集
      *
      * 电影id        1
      * 电影名称      abc
      * 电影描述      abc
      * 电影时长      138 minutes
      * 电影发行日期  1997
      * 电影拍摄日期  1995
      * 电影语言      English
      * 电影类型      a|b|c|...
      * 电影演员      a|b|c|...
      * 电影导演      a|b|c|...
      *
      * 电影的tag    tag1|tag2|tag3|...
      */

    //首先需要将Tag数据集进行处理，处理后的形式为 MID , tag1|tag2|tag3
    import org.apache.spark.sql.functions._
    val newTag = tagDF.groupBy($"mid").agg(concat_ws("|", collect_set($"tag")).as("tags")).select("mid", "tags")

    //将处理后的tag数据和movie数据融合产生新的movie数据
    val movieWithTagsDf = movieDF.join(newTag, Seq("mid", "mid"), "left")

    //声明了ES配置参数
    implicit val esConfig: ESConfig = ESConfig(es_httpHosts, es_transportHosts, es_index, es_cluster_name)

    //将新数据保存在ES
//    storeDataInES(movieWithTagsDf, ES_MOVIE_INDEX)

    //关闭spark
    spark.stop()
  }

  //将数据保存在mongodb
  def storeDataInMongoDB(movie: DataFrame, rating: DataFrame, tag: DataFrame, MONGODB_MOVIE_COLLECTION: String, MONGODB_RATING_COLLECTION: String, MONGODB_TAG_COLLECTION: String, movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    //新建mongodb连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    //如果mongodb中有对应的数据库，那么应该删除
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    //将当前数据写入到mongodb
    movieDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //对数据表建索引
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    //关闭mongodb连接
    mongoClient.close()
  }

  //将数据保存在ES
  def storeDataInES(movieDF: DataFrame, ES_MOVIE_INDEX: String)(implicit esConfig: ESConfig): Unit = {

    //新建一个配置
    val settings: Settings = Settings.builder().put("cluster.name", esConfig.clustername).build()

    //新建一个ES客户端
    val esClient = new PreBuiltTransportClient(settings)

    //需要将TransportHosts添加到esClient中
    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    esConfig.transportHosts.split(",").foreach {
      case REGEX_HOST_PORT(host: String, port: String) =>
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
    }

    //需要清除掉ES中遗留的数据
    if (esClient.admin().indices().exists(new IndicesExistsRequest(esConfig.index)).actionGet().isExists) {
      esClient.admin().indices().delete(new DeleteIndexRequest(esConfig.index))
    }
    esClient.admin().indices().create(new CreateIndexRequest(esConfig.index))

    //将数据写入ES中
    movieDF
      .write
      .option("es.nodes", esConfig.httpHosts)
      .option("es.http.timeout", "100m")
      .option("es.mapping.id", "mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(esConfig.index + "/" + ES_MOVIE_INDEX)
  }
}
