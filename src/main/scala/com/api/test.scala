package com.api

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.config.{Config, ConfigFactory}

object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Ingestion")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    val conf: Config = ConfigFactory.load()
    val settings: AppConf = new AppConf(conf)
    import spark.implicits._
    val url = settings.url
    val get = settings.gpost
    val getP = settings.getP
    val post = settings.post
    println("*******************Api Get***********************************")

    val x = requests.get(url + get)
    val dx: DataFrame = spark.read.json(Seq(x.text).toDS)
    dx.show()
    println("---------------------------------------------------------------------------")
    val p = requests.get(url + getP, params = Map("postId" -> "1"))
    val dy: DataFrame = spark.read.json(Seq(p.text).toDS)
    dy.show()
    println("*****************************Api Post***************************************")
    val a = requests.post(url + post)
    val dz: DataFrame = spark.read.json(Seq(a.text).toDS)
    dz.show()
  }
}
