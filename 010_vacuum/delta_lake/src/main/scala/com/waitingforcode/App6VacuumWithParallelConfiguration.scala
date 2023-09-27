package com.waitingforcode

import org.apache.commons.io.FileUtils

import java.io.File

object App6VacuumWithParallelConfiguration {
  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(checkEnabled = false,
      parallelDeleteEnabled = true, parallelDeleteParallelism = 5, logVacuum = false)
    import sparkSession.implicits._
    val tableName = "app5"

    (0 to 100).toDF("id").writeTo(tableName).using("delta")
      .tableProperty("delta.deletedFileRetentionDuration", "interval 10 seconds")
      .createOrReplace()
    (200 to 205).toDF("id").writeTo(tableName).append()

    sparkSession.sql(s"DELETE FROM ${tableName} WHERE id < 200")

    val dataWarehouseTableLocation = s"${outputDir}/warehouse/$tableName"

    // Create some untracked dummy files and directories
    (0 to 5).foreach(nr => {
      // not empty
      val dummyDir = s"${dataWarehouseTableLocation}/dummy_not_empty_dir_${nr}"
      new File(dummyDir).mkdir()
      FileUtils.writeStringToFile(new File(s"${dummyDir}/1.txt"), "abc", "UTF-8")
      // empty
      new File(s"${dataWarehouseTableLocation}/dummy_empty_dir_${nr}").mkdir()
    })
    println("VACUUMING......")
    Thread.sleep(15000L)
    sparkSession.sql(
      s"""
         |VACUUM delta.`${dataWarehouseTableLocation}` RETAIN 0 HOURS
         |""".stripMargin)
    // Sleep to keep the Spark UI
    Thread.sleep(3000000L)
  }



}
