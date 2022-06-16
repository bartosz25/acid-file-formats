package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode

import java.io.File

object App5ReplaceWhere {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(1)

    import sparkSession.implicits._
    val inputData = (0 to 30).map(nr => (nr, nr*2)).toDF("id", "multiplication_result")
    inputData.write.mode(SaveMode.Overwrite).format("delta").save(outputDir)

    println("=== REPLACING THE TABLE ===")
    // sparkSession.conf.set("spark.databricks.delta.replaceWhere.constraintCheck.enabled", false)
    sparkSession.conf.set("spark.databricks.delta.replaceWhere.constraintCheck.enabled", true)
    //sparkSession.conf.set("spark.databricks.delta.replaceWhere.dataColumns.enabled", false)
    inputData.write.option("replaceWhere", "id <> 1").mode(SaveMode.Overwrite).format("delta").save(outputDir)
    sparkSession.read.format("delta").load(outputDir).show(false)

  }

}
