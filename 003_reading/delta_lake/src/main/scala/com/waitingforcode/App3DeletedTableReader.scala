package com.waitingforcode

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode

import java.io.File

object App3DeletedTableReader {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(2)

    import sparkSession.implicits._
    val inputData = (0 to 30).map(nr => (nr, nr*2)).toDF("id", "multiplication_result")
    inputData.write.mode(SaveMode.Overwrite).format("delta").save(outputDir)

    DeltaTable.forPath(sparkSession, outputDir).delete("id >= 0")

    sparkSession.read.format("delta").load(outputDir).show(false)

    // Despite the delete operation, the data can still be recovered!
    sparkSession.read.format("delta").option("versionAsOf", "0").load(outputDir)
      .show(false)
  }

}
