package com

import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
import org.apache.spark.sql.SparkSession

import java.sql.Timestamp
import java.time.Instant

package object waitingforcode {

  val outputDir = "/tmp/acid-file-formats/006_writing/iceberg"
  def getIcebergSparkSession() = {
    SparkSession.builder()
      .appName("Compaction").master("local[*]")
      .withExtensions(new IcebergSparkSessionExtensions())
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", s"file://${outputDir}")
      .enableHiveSupport()
      .getOrCreate()
  }

  case class Letter(id: Int, lowerCase: String, upperCase: String, nestedLetter: NestedLetter,
                    creationTime: Timestamp = Timestamp.from(Instant.now()))

  case class NestedLetter(key: String, value: String)
}
