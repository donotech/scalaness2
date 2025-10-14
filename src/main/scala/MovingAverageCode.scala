package org.bdec.training
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.functions._

case class InputData(key: String, value: Double, eventTime: java.sql.Timestamp)
case class OutputData(key: String, movingAvg: Double)

object MovingAverageCode {

  def stateUpdateFunc(key: String, values: Iterator[InputData], state: GroupState[Double]): Iterator[OutputData] = {
    val movingAvg = values.map(_.value).sum / values.size
    val prevAvg = if (state.exists) state.get else 0.0

    if (prevAvg == 0.0 || movingAvg > prevAvg * 1.10) {
      state.update(movingAvg)
      Iterator(OutputData(key, movingAvg))
    } else {
      Iterator.empty
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("MovingAverageStream").getOrCreate()
    import spark.implicits._

    // Read streaming input (example: from Kafka or socket)
    val inputStream = spark.readStream
      .format("file")
      .option("your_options", "...")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS DOUBLE)", "CAST(eventTime AS TIMESTAMP)")
      .as[InputData]

    // Define a function to calculate and compare moving average using state


    val result = inputStream
      .groupByKey(_.key)
      .flatMapGroupsWithState(OutputMode.Update, GroupStateTimeout.NoTimeout())(stateUpdateFunc)

    // Write output to sink
    val query = result.writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()
  }

}
