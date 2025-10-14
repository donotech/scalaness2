package org.bdec.training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._

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
//
//    val dfNews = sparkSession.readStream.option("header","false")
//      .schema(newsSchema).csv("/home/devraj/sparktrain/news")
//      .withWatermark(eventTime = "news_date", "1 minutes")
//    val dfPrice = sparkSession.readStream
//      .schema(priceSchema).csv("/home/devraj/sparktrain/price")
//      .withWatermark(eventTime = "price_date", delayThreshold = "1 minutes")

    val dfNews = sparkSession.readStream.option("header","false")
          .schema(newsSchema).csv("/home/devraj/sparktrain/news")
      .withColumn("current_time", current_timestamp())
    val dfPrice = sparkSession.readStream
          .schema(priceSchema).csv("/home/devraj/sparktrain/price")
    //count the negative words in the news ("bad", "not good", "down")
    //count the number of negative news (for 10 mins) if count(negative news) > 10
    //pick the price trend - if it is dipping (moving average is coming down) 5 min price > min price
    //then send a message to kafka  - write to a disk
//    val dfNewsSliding = dfNews.groupBy(window(col("current_time"),"5 minutes")).agg(Map()) //, "5 minutes"))
//    val dfJoined = dfPrice.join(dfNews, Seq("ticker"))
//
//    val query = dfJoined.writeStream.format("console").outputMode("append").start()
//    val query2 = dfPrice.writeStream.format("kafka").outputMode("append").start()
//    val query3 = dfNewsSliding.writeStream.format("jdbc").outputMode("complete").start()
//    sparkSession.streams.awaitAnyTermination()
  }

}
