package com.api


import org.apache.spark.sql.{DataFrame, SparkSession}


object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Ingestion")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val x = requests.get("https://testimonialapi.toolcarton.com/api")

    val dx: DataFrame = spark.read.json(Seq(x.text).toDS)
    dx.show()
    println("------------------------------------------------")
    val dd: String =x.text().mkString
    val a = dd.replace("[", "")
    val b = a.replace("]", "")


    val dz: DataFrame = dx.rdd.map(r =>
    Converter.jsonToCase(b)
    ).toDF()
    dz.show()
    print("----------------------------------------------------------")/*
    val dj: DataFrame = dx.rdd.map(r => ApiTest(
      id = r.getLong(3),
      name = r.getString(7),
      location = r.getString(4),
      designation = r.getString(2),
      avatar = r.getString(1),
      message = r.getString(6),
      lorem = r.getString(5),
      rating = r.getDouble(8),
      audio = r.getString(0)
    )).toDF()
    dj.show()*/
  }
}
