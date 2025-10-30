import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object AzureEventHubReader {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("EventHubStreaming")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val eventHubNamespace = "nesstrain"
    val eventHubName = "nesstopic"
    val connectionString = "Endpoint=sb://nesstrain.servicebus.windows.net/;SharedAccessKeyName=producerconsumer;SharedAccessKey=<>>=;EntityPath=nesstopic"

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", s"$eventHubNamespace.servicebus.windows.net:9093")
      .option("subscribe", eventHubName)
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.jaas.config",
        s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="$$ConnectionString" password="$connectionString";""")
      .load()

    val query = df.selectExpr("CAST(value AS STRING)")
      .writeStream
      .format("console")
      .start()

    query.awaitTermination()
//    val urlStr = "jdbc:sqlserver://kraftsynapse1-ondemand.sql.azuresynapse.net:1433;database=master;user=sqladmin@kraftsynapse1;password={your_password_here};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;"
//    spark.read.jdbc(url = urlStr, table = "your table name")

  }
}
