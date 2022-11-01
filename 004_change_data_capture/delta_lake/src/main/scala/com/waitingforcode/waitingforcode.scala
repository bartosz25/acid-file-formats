package com

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.sql.SparkSession

package object waitingforcode {

  val outputDir = "/tmp/acid-file-formats/004_change_data_capture/delta_lake"

  def getDeltaLakeSparkSession(cores: Int = 1) = {
    SparkSession.builder()
      .appName("Reading").master(s"local[${cores}]")
      .withExtensions(new DeltaSparkSessionExtension())
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", s"${outputDir}/warehouse")
      .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
      .getOrCreate()
  }

  case class User(id: Int, login: String, isActive: Boolean)

}
