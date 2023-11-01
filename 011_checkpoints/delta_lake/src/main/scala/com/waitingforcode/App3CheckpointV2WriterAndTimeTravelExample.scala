package com.waitingforcode

import org.apache.commons.io.FileUtils

import java.io.File

object App3CheckpointV2WriterAndTimeTravelExample {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))
    val sparkSession = getDeltaLakeSparkSession(2)
    import sparkSession.implicits._
    val tableName = "app3"

    sparkSession.sql(
      s"""
         |CREATE TABLE ${tableName} (id int) USING delta
         | TBLPROPERTIES ('delta.checkpointInterval' = 2, 'delta.checkpointPolicy' = 'v2')
         |""".stripMargin)

    (1 to 5).foreach(_ => {
      (0 to 100).toDF("id").writeTo(tableName).append()
    })
    // In the end of this operation we should have 5 data commits with 2 checkpoints:
    // 2 => 1 and 2 commits
    // 4 => 1, 2, 3, and 4 commits data

    // Let's test now the impact of the checkpointing on the time-travel

    // Run the `App3CheckpointWorkingV2ReaderExample` to see if the version 2 is still
    // reachable. At this moment, the checkpoint didn't clean any files up, so you shouldn't
    // see an error.

    // To do so, change the time of your PC and advance by 1 month and 2 days, so that
    // the current time is after the default retention configuration of 30 days

    // Later, run the App3CheckpointV2ReaderExample. It'll add 5 new commits to the log
    // but also remove all yesterday's checkpoints

    // After that, the code uses time-travel to restore the table at the 2nd commit
    // This operation should logically fail since there is no file for the 2nd commit log.
    // On the other side, if you issue a `SELECT * FROM app3` query, you will see it returns
    // all the 1 010 rows - so the rows from yesterday and today. It proves that the checkpointing
    // doesn't impact the data. Put differently, the data remains valid despite the checkpointing
    // as long as you don't decide otherwise.
  }

}

object App3CheckpointWorkingV2ReaderExample {
  def main(args: Array[String]): Unit = {
    val sparkSession = getDeltaLakeSparkSession(2)
    import sparkSession.implicits._
    val tableName = "app3"

    val rowsV2 = sparkSession.read
        .format("delta")
        .option("versionAsOf", "2")
        .load(s"${outputDir}/warehouse/${tableName}").count()

    println(s"Rows v2=${rowsV2}")
    sparkSession.sql(s"SELECT COUNT(*) AS total_rows FROM ${tableName}").show()
  }

}

object App3CheckpointNotWorkingV2ReaderExample {
  def main(args: Array[String]): Unit = {
    val sparkSession = getDeltaLakeSparkSession(2)
    import sparkSession.implicits._
    val tableName = "app3"

    (1 to 5).foreach(_ => {
      (0 to 100).toDF("id").writeTo(tableName).append()
    })

    try {
      val rowsV2 = sparkSession.read
        .format("delta")
        .option("versionAsOf", "2")
        .load(s"${outputDir}/warehouse/${tableName}").count()
      println(s"Rows v2=${rowsV2}")
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }

    sparkSession.sql(s"SELECT COUNT(*) AS total_rows FROM ${tableName}").show()
  }

}