package com

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.sql.SparkSession

package object waitingforcode {

  val outputDir = "/tmp/table-file-formats/011_checkpoints/delta_lake"
  val dataWarehouseBaseDir = s"${outputDir}/warehouse"
  System.setProperty("derby.system.home", dataWarehouseBaseDir)
  def getDeltaLakeSparkSession(cores: Int = 1) = {
    SparkSession.builder()
      .appName("Writing").master(s"local[${cores}]")
      .withExtensions(new DeltaSparkSessionExtension())
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", dataWarehouseBaseDir)
      .enableHiveSupport()
      .getOrCreate()
  }

}