package com.waitingforcode

import org.apache.commons.io.FileUtils

import java.io.File

object App5VacuumDryRunForUntrackedFiles {
  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(checkEnabled = false)
    import sparkSession.implicits._
    val tableName = "app5"

    (0 to 100).toDF("id").writeTo(tableName).using("delta")
      .tableProperty("delta.deletedFileRetentionDuration", "interval 10 seconds")
      .createOrReplace()
    (200 to 205).toDF("id").writeTo(tableName).append()

    sparkSession.sql(s"DELETE FROM ${tableName} WHERE id < 200")

    val dataWarehouseTableLocation = s"${outputDir}/warehouse/$tableName"
    sparkSession.sql(
      s"""
      |VACUUM delta.`${dataWarehouseTableLocation}` RETAIN 0 HOURS DRY RUN
      |""".stripMargin)

    // Create some untracked dummy files
    // not empty
    val dummyDir = s"${dataWarehouseTableLocation}/dummy_not_empty_dir"
    new File(dummyDir).mkdir()
    FileUtils.writeStringToFile(new File(s"${dummyDir}/1.txt"), "abc", "UTF-8")
    // empty
    new File(s"${dataWarehouseTableLocation}/dummy_empty_dir").mkdir()

    // Rerun the command
    sparkSession.sql(
      s"""
         |VACUUM delta.`${dataWarehouseTableLocation}` RETAIN 0 HOURS DRY RUN
         |""".stripMargin)

    // Dry runs should print, respectively:
    // Found 3 files (1618 bytes) and directories in a total of 1 directories that are safe to delete.
    // Found 5 files (1621 bytes) and directories in a total of 3 directories that are safe to delete.
  }

}