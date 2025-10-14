package org.bdec.training

import org.apache.spark.sql.SparkSession

object SparkSubmitDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("sparktrain")
      .master("local[*]")
      .getOrCreate()

    val fileName = args(0)

    val df = sparkSession.read.option("inferSchema", "true")
      .option("header", "true")
      .csv(fileName)
    df.show()
    df.printSchema()
  }
}
