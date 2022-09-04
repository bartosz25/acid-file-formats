package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode

import java.io.File

object App1SimpleReader {

  def main(args: Array[String]): Unit = {
    val sparkSession = getDeltaLakeSparkSession(1)
    FileUtils.deleteDirectory(new File(outputDir))

    import sparkSession.implicits._
    (0 to 1).foreach(transactionNumber => {
      val inputData = (0 to 30).map(nr => (s"${transactionNumber}_${nr}", nr*2)).toDF("id", "multiplication_result")
      val saveMode = if (transactionNumber == 0) {
        SaveMode.Overwrite
      } else {
        SaveMode.Append
      }
      inputData.write.mode(saveMode).format("delta").save(outputDir)
    })

    sparkSession.read.format("delta").load(outputDir).show(false)
  }

}
