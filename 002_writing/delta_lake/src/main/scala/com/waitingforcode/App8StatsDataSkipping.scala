package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.delta.DeltaLog

import java.io.File

object App8StatsDataSkipping {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(1)

    sparkSession.conf.set("spark.databricks.io.skipping.stringPrefixLength", "20")
    import sparkSession.implicits._
    val inputData = (0 to 30).map(nr => (nr, nr*2, (0 to nr).mkString("")))
      .toDF("id", "multiplication_result", "concat_numbers")
    inputData.show(false)
    inputData.write.mode(SaveMode.Overwrite).format("delta").save(outputDir)

    // The implementation based on:
    val deltaLog = DeltaLog.forTable(sparkSession, outputDir)
    val snapshot = deltaLog.snapshot
    snapshot.allFiles
      .select("path", "stats")
      .show(false)
  }

}
