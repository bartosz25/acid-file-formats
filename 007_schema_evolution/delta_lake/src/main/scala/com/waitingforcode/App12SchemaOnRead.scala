package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, functions}

import java.io.File

object App12SchemaOnRead {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(2, true)
    import sparkSession.implicits._

    val inputData = (0 to 2).map(nr => (nr, nr * 2)).toDF("id", "multiplication_result")
    inputData.write.mode(SaveMode.Overwrite).format("delta").save(outputDir)

    val reader = sparkSession.read.format("delta")
      .load(outputDir)

    val inputDataWithDivisionResult = (0 to 2).map(nr => (nr, nr.toDouble * 2.toDouble)).toDF("id", "multiplication_result")
    inputDataWithDivisionResult.write.option("overwriteSchema", true).mode(SaveMode.Overwrite).format("delta").save(outputDir)

    reader.show(false)
  }

}
