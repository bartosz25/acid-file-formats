package com

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension

import java.sql.Timestamp
import java.time.Instant

package object waitingforcode {

  val outputDir = "/tmp/acid-file-formats/001_storage_layout/hudi"

  def getHudiSparkSession() = {
    SparkSession.builder()
      .appName("Storage layout").master("local[*]")
      .withExtensions(new HoodieSparkSessionExtension())
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()
  }

  case class Letter(id: String, lowerCase: String, creationTime: Timestamp = Timestamp.from(Instant.now()))

}
