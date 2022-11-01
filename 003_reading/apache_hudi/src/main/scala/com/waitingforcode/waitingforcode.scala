package com

import org.apache.spark.sql.SparkSession

import java.sql.Timestamp
import java.time.Instant

package object waitingforcode {

  val outputDir = "/tmp/acid-file-formats/007_writing/hudi"
  def getHudiSparkSession() = {
    SparkSession.builder()
      .appName("Writing path").master("local[*]")
      //.withExtensions(new HoodieSparkSessionExtension())
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
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


  case class Letter(id: Long, upperCase: String, lowerCase: String,
                    category: String = "important",
                    creationTime: Timestamp = Timestamp.from(Instant.now()))
}
