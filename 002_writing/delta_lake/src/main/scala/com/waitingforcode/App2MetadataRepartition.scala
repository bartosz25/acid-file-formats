package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode

import java.io.File

object App2MetadataRepartition {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession()

    import sparkSession.implicits._
    val inputData = (0 to 1000).map(nr => (nr, nr*2)).toDF("id", "multiplication_result")

    inputData.write.mode(SaveMode.Overwrite).format("delta").save(outputDir)

    sparkSession
      .read.format("delta").load(outputDir)
      .repartition(2)
      .write.option("dataChange", false)
      .format("delta").mode(SaveMode.Overwrite).save(outputDir)


  }

}
