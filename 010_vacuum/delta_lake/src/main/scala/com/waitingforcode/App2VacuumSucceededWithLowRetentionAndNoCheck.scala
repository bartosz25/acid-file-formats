package com.waitingforcode

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils

import java.io.File

object App2VacuumSucceededWithLowRetentionAndNoCheck {
  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(checkEnabled = false)
    import sparkSession.implicits._
    val tableName = "app2"

    (0 to 100).toDF("id").writeTo(tableName).using("delta")
      .tableProperty("delta.deletedFileRetentionDuration", "interval 3 weeks")
      .createOrReplace()


    (200 to 205).toDF("id").writeTo(tableName).append()

    sparkSession.sql(s"DELETE FROM ${tableName} WHERE id < 200")

    DeltaTable.forName(tableName)
    sparkSession.sql(s"DESCRIBE DETAIL $tableName")
      .show(truncate = false)

    sparkSession.sql(
      s"""
         |VACUUM delta.`${outputDir}/warehouse/$tableName` RETAIN 0 HOURS
         |""".stripMargin)

    sparkSession.sql(s"SELECT * FROM ${tableName}").show()

    DeltaTable.forName(tableName).history().show(false)
  }



}
