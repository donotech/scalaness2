package org.bdec.training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

object StreamStreamJoin {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("sparktrain")
      .master("local[*]")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("WARN")

    val newsSchema = StructType(Array(
      StructField(name="ticker", StringType),
      StructField(name="news",StringType),
      StructField(name="news_date",TimestampType)
    ))

    val priceSchema = StructType(Array(
      StructField(name="ticker", StringType),
      StructField(name="price_date",TimestampType),
      StructField(name="Price",DoubleType)
    ))

    val dfNews = sparkSession.readStream.option("header","false")
      .schema(newsSchema).csv("/home/devraj/sparktrain/news")
      .withWatermark(eventTime = "news_date", "10 minutes")
    val dfPrice = sparkSession.readStream
      .schema(priceSchema).csv("/home/devraj/sparktrain/price")
      .withWatermark(eventTime = "price_date", delayThreshold = "10 minutes")

    val dfJoined = dfPrice.join(dfNews, Seq("ticker"))

    val query = dfJoined.writeStream.format("console").outputMode("append").start()
    query.awaitTermination()
  }

}
