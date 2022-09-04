package com

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.sql.SparkSession

import java.sql.Timestamp
import java.time.Instant

package object waitingforcode {

  val outputDir = "/tmp/acid-file-formats/003_reading/delta_lake"

  def getDeltaLakeSparkSession(cores: Int = 1) = {
    SparkSession.builder()
      .appName("Reading").master(s"local[${cores}]")
      .withExtensions(new DeltaSparkSessionExtension())
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", s"${outputDir}/warehouse")

      .config("spark.databricks.delta.stats.skipping", true)
      .getOrCreate()
  }

  case class Letter(id: Int, lowerCase: String, upperCase: String, nestedLetter: NestedLetter,
                    creationTime: Timestamp = Timestamp.from(Instant.now()))

  case class NestedLetter(key: String, value: String)
}
