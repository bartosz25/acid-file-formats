package com.waitingforcode

import org.apache.commons.io.FileUtils

import java.io.File

object App4VacuumDryRun {
  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(checkEnabled = false)
    import sparkSession.implicits._
    val tableName = "app4"

    (0 to 100).toDF("id").writeTo(tableName).using("delta")
      .tableProperty("delta.deletedFileRetentionDuration", "interval 10 seconds")
      .createOrReplace()

    (200 to 205).toDF("id").writeTo(tableName).append()

    sparkSession.sql(s"DELETE FROM ${tableName} WHERE id < 200")

    sparkSession.sql(s"DESCRIBE DETAIL $tableName")
      .show(truncate = false)

    sparkSession.sql(
      s"""
         |VACUUM delta.`${outputDir}/warehouse/$tableName` RETAIN 0 HOURS DRY RUN
         |""".stripMargin)
  }

}