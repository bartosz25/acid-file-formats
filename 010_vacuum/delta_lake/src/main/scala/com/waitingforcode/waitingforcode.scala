package com

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.spark.sql.SparkSession

package object waitingforcode {

  val outputDir = "/tmp/table-file-formats/010_vacuum/delta_lake"

  def getDeltaLakeSparkSession(checkEnabled: Boolean = true,
                               parallelDeleteEnabled: Boolean = false,
                               parallelDeleteParallelism: Int = 200,
                               logVacuum: Boolean = true): SparkSession = {
    SparkSession.builder().master("local[2]")
      .withExtensions(new DeltaSparkSessionExtension())
      .config("spark.databricks.delta.retentionDurationCheck.enabled", checkEnabled)
      .config("spark.databricks.delta.vacuum.parallelDelete.enabled", parallelDeleteEnabled)
      .config("spark.databricks.delta.vacuum.parallelDelete.parallelism", parallelDeleteParallelism)
      .config("spark.databricks.delta.vacuum.logging.enabled", logVacuum)
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", s"${outputDir}/warehouse")
      .getOrCreate()
  }

}