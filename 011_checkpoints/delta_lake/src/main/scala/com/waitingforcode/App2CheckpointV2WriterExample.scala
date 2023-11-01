package com.waitingforcode

import org.apache.commons.io.FileUtils

import java.io.File

object App2CheckpointV2WriterExample {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))
    val sparkSession = getDeltaLakeSparkSession(2)
    import sparkSession.implicits._
    val tableName = "app2"

    sparkSession.sql(
      s"""
         |CREATE TABLE ${tableName} (id int) USING delta
         | TBLPROPERTIES ('delta.checkpointInterval' = 2, 'delta.checkpointPolicy' = 'v2')
         |""".stripMargin)

    (0 until 5).foreach(_ => {
      (0 to 100).toDF("id").writeTo(tableName).append()
    })
  }

}

object App2CheckpointV2ReaderExample {
  def main(args: Array[String]): Unit = {
    val sparkSession = getDeltaLakeSparkSession(2)
    import sparkSession.implicits._
    val tableName = "app2"

    sparkSession.read.format("delta").load(s"${outputDir}/warehouse/${tableName}")
      .show()
  }

}