package com.waitingforcode

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode

import java.io.File

object App4UpdatedTableReader {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(2)

    import sparkSession.implicits._
    val inputData = (0 to 30).map(nr => (nr, nr*2)).toDF("id", "multiplication_result")
    inputData.write.mode(SaveMode.Overwrite).format("delta").save(outputDir)

    DeltaTable.forPath(sparkSession, outputDir).updateExpr("id > 10", Map("id" -> "id * 100"))

    sparkSession.read.format("delta").load(outputDir).show(false)
  }

}
