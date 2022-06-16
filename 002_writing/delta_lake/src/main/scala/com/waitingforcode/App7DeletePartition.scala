package com.waitingforcode

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode

import java.io.File

object App7DeletePartition {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(1)

    import sparkSession.implicits._
    val inputData = (0 to 30).map(nr => (nr, nr*2, nr % 2 == 0)).toDF("id", "multiplication_result", "is_even")
    inputData.write.partitionBy("is_even").mode(SaveMode.Overwrite).format("delta").save(outputDir)

    println("=== REPLACING THE TABLE PARTITION ===")
    val deltaTable = DeltaTable.forPath(sparkSession, outputDir)
    //deltaTable.delete("is_even IS TRUE")
    deltaTable.delete("id > 3")

    sparkSession.read.format("delta").load(outputDir).show(false)
  }

}
