package com

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension

import java.sql.Timestamp
import java.time.Instant

package object waitingforcode {

  val outputDir = "/tmp/acid-file-formats/007_writing/hudi"
  def getHudiSparkSession() = {
    SparkSession.builder()
      .appName("Writing path").master("local[*]")
      .withExtensions(new HoodieSparkSessionExtension())
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.warehouse.dir", s"${outputDir}/warehouse")
      .getOrCreate()
  }

  lazy val hudiSparkSession = getHudiSparkSession()
  import hudiSparkSession.implicits._
  val inputData = Seq(
    Letter(1, "A", "a", "important"),
    Letter(2, "B", "b", "important"),
    Letter(3, "C", "c", "important")
  ).toDF


  case class Letter(id: Long, lowerCase: String, upperCase: String,
                    category: String = "important",
                    creationTime: Timestamp = Timestamp.from(Instant.now()))
}
