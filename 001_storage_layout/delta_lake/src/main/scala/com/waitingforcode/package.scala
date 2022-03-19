package com

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.sql.SparkSession

import java.sql.Timestamp
import java.time.Instant

package object waitingforcode {

  val outputDir = "/tmp/acid-file-formats/001_storage/delta_lake"

  def getDeltaLakeSparkSession() = {
    SparkSession.builder()
      .appName("Storage layout").master("local[*]")
      .withExtensions(new DeltaSparkSessionExtension())
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      // Enable the support to keep the tables alive
      .enableHiveSupport()
      .getOrCreate()
  }

  case class Letter(id: String, lowerCase: String, creationTime: Timestamp = Timestamp.from(Instant.now()))

}
