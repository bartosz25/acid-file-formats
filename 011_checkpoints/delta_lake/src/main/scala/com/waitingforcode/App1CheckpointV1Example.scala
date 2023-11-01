package com.waitingforcode

import org.apache.commons.io.FileUtils

import java.io.File

object App1CheckpointV1Example {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))
    val sparkSession = getDeltaLakeSparkSession(2)
    import sparkSession.implicits._
    val tableName = "app1"

    sparkSession.sql(
      s"""
         |CREATE TABLE ${tableName} (id int) USING delta
         | TBLPROPERTIES ('delta.checkpointInterval' = 5)
         |""".stripMargin)

    (0 until 5).foreach(_ => {
      (0 to 100).toDF("id").writeTo(tableName).append()
    })

    sparkSession.read.parquet(s"${outputDir}/warehouse/${tableName}/_delta_log/00000000000000000005.checkpoint.parquet")
      .show()
  }

}
object App1CheckpointV1ReaderExample {

  def main(args: Array[String]): Unit = {
    val sparkSession = getDeltaLakeSparkSession(2)
    import sparkSession.implicits._
    val tableName = "app1"

    sparkSession.read.format("delta").load(s"${outputDir}/warehouse/${tableName}")
      .show()
  }

}
