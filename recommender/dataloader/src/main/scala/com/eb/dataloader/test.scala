package com.eb.dataloader

object test {
  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://linux:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "linux:9200",
      "es.transportHosts" -> "linux:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "es-cluster"
    )
    config.get("spark.cores").get
    val str: String = config("spark.cores")
    println(str)
  }
}
