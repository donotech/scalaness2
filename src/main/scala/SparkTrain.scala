package org.bdec.training

import org.apache.spark.sql.SparkSession

case class SubjectMarks(subject: String, marks: Long)
case class Element(English: Int, Maths: Int)
case class StudentSpark(name: String, grade: Long, scores: Array[Element])
case class StudentTotal(name: String, total: Long)
case class Student2Spark(name: String, grade: Long, scores: Array[SubjectMarks])
case class StudentFlat(name: String, grade: Long, subject: String, marks: Long)

object SparkTrain {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("sparktrain")
      .master("local[*]")
      .getOrCreate()
    //sparkSession.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //import sparkSession.implicits._

    val df = sparkSession.read.option("inferSchema", "true")
      .option("header", "true")
      .csv("/home/devraj/scalaness2/random.csv")
    df.show()
    df.printSchema()
    //val flattenedScores = df.select("scores").as[Array[Element]].flatMap(e => e)

//    val ds = sparkSession.read.json("/home/devraj/IdeaProjects/scalaness/score.json").as[StudentSpark]
//    val flattenedScores = ds.map(s => {
//      val element = s.scores(0)
//      val total = element.Maths + element.English
//      StudentTotal(s.name, total)
//    })
//    //flattenedScores.show()
//    val ds3 = flattenedScores.map(s => {
//      StudentTotal(s.name, s.total + 10)
//    })
//    ds3.show()
//
//    val ds4 = ds.map(s => s.copy(grade = s.grade + 1))
//    ds4.show()
//    ds4.cache()

//    val ds2 = sparkSession.read.json("/home/devraj/IdeaProjects/scalaness/score2.json").as[Student2Spark]
//    val dsFlattened = ds2.flatMap(s => {
//      s.scores.map(score => StudentFlat(s.name, s.grade, score.subject, score.marks))
//    })
//
//    dsFlattened.show()
//    Thread.sleep(1000000)
  }


}
