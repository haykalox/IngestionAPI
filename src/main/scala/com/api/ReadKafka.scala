package com.api

import org.apache.spark.sql.SparkSession

object ReadKafka {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Ingestion")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "0.0.0.0:6667")
      .option("subscribe", "streaming-test")
      .load()

    val newDF = df.selectExpr("CAST(value AS STRING)")

    val query = newDF.writeStream
      .outputMode("append")
      .format("console")
      .start()
    query.awaitTermination()
  }
}
