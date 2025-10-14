package org.bdec.training

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object StreamingCSVRead {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("sparktrain")
      .master("local[*]")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("WARN")

    val csvSchema = StructType(Array(
      StructField(name="time_id", IntegerType),
      StructField(name="product",StringType),
      StructField(name="num_items", IntegerType),
      StructField(name="double_items", IntegerType)
    ))
    //sparkSession.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //import sparkSession.implicits._

    val csvProductSchema = StructType(Array(
      StructField(name="product", StringType),
      StructField(name="type",StringType),
      StructField(name="sub_type", StringType)
    ))
    val df = sparkSession.readStream.schema(csvSchema)
      .csv("/home/devraj/sparkdir")

    val productDf = sparkSession.read.schema(csvProductSchema).csv()
    productDf.show()

    val joinedDf = df.join(productDf, Seq("product"), "right_outer")
    val query = joinedDf.writeStream.format("console").outputMode("append").start()
    query.awaitTermination()
//    println(s"Count ***** " + df.count().toString + "  ****")
  }

}
