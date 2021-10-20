package com.api

import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}


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

    val data = df.selectExpr("CAST(value AS STRING)")
/*
    data.writeStream
      .format("text")
      .outputMode("append")
      .option("checkpointLocation", "/home/datakafka/delete")
      .option("path","/home/datakafka/test_kafka")
      .start()
      .awaitTermination()
*/

    val output_stream_df = data.writeStream.format("hive")
      .queryName("hiveSink")
      .option("database", "default")
      .option("table", "kafkaTest")
      .option("checkpointLocation", "/home/datakafka/test_kafka")
      .option("spark.acid.streaming.log.metadataDir", "/home/datakafka/test_kafka/spark-acid")
       .option("metastoreUri", "thrift://hive-metastore:9083")
      .start()

    output_stream_df.awaitTermination()


/*
    val query = df.writeStream
      .outputMode("append")
      .format("console")
      .start()
    query.awaitTermination()*/
  }
}
