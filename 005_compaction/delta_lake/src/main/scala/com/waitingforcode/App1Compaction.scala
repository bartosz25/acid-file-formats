package com.waitingforcode

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.delta.DeltaLog

import java.io.File

object App1Compaction {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(2)

    import sparkSession.implicits._
    val inputData = (0 to 2).map(nr => (nr, nr * 2)).toDF("id", "multiplication_result")
    inputData.write.mode(SaveMode.Overwrite).format("delta").save(outputDir)
    inputData.write.mode(SaveMode.Append).format("delta").save(outputDir)
    inputData.write.mode(SaveMode.Append).format("delta").save(outputDir)

    val deltaLog = DeltaLog.forTable(sparkSession, outputDir)
    val snapshot = deltaLog.unsafeVolatileSnapshot
    snapshot.allFiles.show(false)

    val numberOfFiles = 1
    sparkSession.read
      .format("delta")
      .load(outputDir)
      .repartition(numberOfFiles)
      .write
      .option("dataChange", "false")
      .format("delta")
      .mode("overwrite")
      .save(outputDir)

    val snapshotAfterCompaction = deltaLog.unsafeVolatileSnapshot
    snapshotAfterCompaction.allFiles.show(false)

    val deltaTable = DeltaTable.forPath(sparkSession, outputDir)
    deltaTable.history
      .show(false)
  }

}
