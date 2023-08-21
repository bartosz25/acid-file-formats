package com

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.sql.SparkSession

package object waitingforcode {

  val outputDir = "/tmp/table-file-formats/008_commit/delta_lake"

  def getDeltaLakeSparkSession(cores: Int = 1) = {
    SparkSession.builder()
      .appName("Writing").master(s"local[${cores}]")
      .withExtensions(new DeltaSparkSessionExtension())
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", s"${outputDir}/warehouse")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", s"${outputDir}/warehouse-local")
      .getOrCreate()
  }

}