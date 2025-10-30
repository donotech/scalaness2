package org.bdec.training

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql.SparkSession

class Modelnferencer {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = null

    val rawDf  = spark.read.parquet("")
    val requiredFields = rawDf.filter("Suburb='Abbotsford'").filter("type='u'")
      .drop("Suburb", "Address", "Method", "Type", "SellerG", "Date",
        "Postcode", "CouncilArea", "Lattitude", "Distance", "PropertyCount",
        "Longtitude", "RegionName")
    val fieldsForTraining = requiredFields
      .na
      .drop.selectExpr("cast(Rooms as double)", "cast(Price as double)",
        "cast(Bedroom2 as double)", "cast(Bathroom as double)", "cast(Car as double)", "cast(Landsize as double)",
        "cast(BuildingArea as double)", "cast(YearBuilt as double)")

    val inputCols = Array[String]("Rooms", "Bedroom2", "Bathroom", "Car", "Landsize", "BuildingArea", "YearBuilt")
    val assembler = new VectorAssembler().setInputCols(inputCols).setOutputCol("features")
    val finalDf = assembler.transform(fieldsForTraining)
    val model = LinearRegressionModel.load("/home/devraj/trainedlr_model")
    val predicted_df = model.transform(finalDf)
//    under prediction column the predicted price will be there
    predicted_df.write.parquet("")
  }
}
