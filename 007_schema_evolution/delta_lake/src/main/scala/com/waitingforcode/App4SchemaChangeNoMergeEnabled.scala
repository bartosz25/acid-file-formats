package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, functions}

import java.io.File

object App4SchemaChangeNoMergeEnabled {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(2)

    import sparkSession.implicits._
    val inputData = (0 to 2).map(nr => (nr, nr * 2)).toDF("id", "multiplication_result")
    inputData.write.mode(SaveMode.Overwrite).format("delta").save(outputDir)

    val inputDataWithNewColumn = inputData.withColumn("generation_time", functions.current_timestamp())
    inputDataWithNewColumn.write.mode(SaveMode.Append).format("delta").save(outputDir)
  }

}
