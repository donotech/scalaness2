package org.bdec.training

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.Future.never.onComplete
import scala.util.{Failure, Success}


object Main {
  def fetchData(): Future[String] = Future {
    Thread.sleep(5000) // Simulate a long-running operation
    // println("2nd thread")
    "Complete"
    }


  def main(args: Array[String]): Unit = {
    println("Hello world!")
    val dataFuture: Future[String] = fetchData()
//    println("Main thread")
    dataFuture.onComplete {
      case Success(data) => println(s"Received data: $data")
      case Failure(ex) => println(s"Error fetching data: ${ex.getMessage}")
    }
    Await.result(dataFuture, scala.concurrent.duration.Duration.Inf) }
}