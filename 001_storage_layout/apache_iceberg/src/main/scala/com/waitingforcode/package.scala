package com

import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
import org.apache.spark.sql.SparkSession

import java.sql.Timestamp
import java.time.Instant

package object waitingforcode {

  def getIcebergSparkSession() = {
    SparkSession.builder()
      .appName("Storage layout").master("local[*]")
      .withExtensions(new IcebergSparkSessionExtensions())
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", "file:///tmp/acid-file-formats/001_storage_layout/catalog")
      .getOrCreate()
  }

  case class Letter(id: String, lowerCase: String, creationTime: Timestamp = Timestamp.from(Instant.now()))

}
