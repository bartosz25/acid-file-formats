package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode

import java.io.File

object App6ReplaceWherePartition {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(1)

    import sparkSession.implicits._
    val inputData = (0 to 30).map(nr => (nr, nr*2, nr % 2 == 0)).toDF("id", "multiplication_result", "is_even")
    inputData.write.partitionBy("is_even").mode(SaveMode.Overwrite).format("delta").save(outputDir)

    println("=== REPLACING THE TABLE PARTITION ===")
    val newPartitionValues = inputData
      //.filter("is_even IS TRUE")
      .selectExpr("id", "(multiplication_result * 10) AS multiplication_result", "is_even")
    newPartitionValues.write.option("replaceWhere", "is_even IS TRUE")
      .mode(SaveMode.Overwrite).format("delta").save(outputDir)

    sparkSession.read.format("delta").load(outputDir).show(false)
  }

}
