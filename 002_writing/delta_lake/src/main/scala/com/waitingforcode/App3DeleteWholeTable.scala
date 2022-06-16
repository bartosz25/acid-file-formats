package com.waitingforcode

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode

import java.io.File

object App3DeleteWholeTable {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(2)

    import sparkSession.implicits._
    val inputData = (0 to 2).map(nr => (nr, nr*2)).toDF("id", "multiplication_result")
    inputData.write.mode(SaveMode.Overwrite).format("delta").save(outputDir)

    Seq(true, false).foreach(isOverwrite => {
      if (isOverwrite) {
        println("=== OVERWRITING THE TABLE ===")
        inputData.write.mode(SaveMode.Overwrite).format("delta").save(outputDir)
        sparkSession.read.format("delta").load(outputDir).show(false)
      } else {
        println("=== DELETING THE TABLE ===")
        val deltaTable = DeltaTable.forPath(sparkSession, outputDir)
        deltaTable.delete()
        (4 to 6).map(nr => (nr, nr*2)).toDF("id", "multiplication_result")
          .write.mode(SaveMode.Append).format("delta").save(outputDir)
        sparkSession.read.format("delta").load(outputDir).show(false)
      }
    })

  }

}
