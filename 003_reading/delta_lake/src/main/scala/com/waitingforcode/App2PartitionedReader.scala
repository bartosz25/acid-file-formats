package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode

import java.io.File

object App2PartitionedReader {

  def main(args: Array[String]): Unit = {
   FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(1)

    import sparkSession.implicits._
    (0 to 1).foreach(transactionNumber => {
      val saveMode = if (transactionNumber == 0) {
        SaveMode.Overwrite
      } else {
        SaveMode.Append
      }
      val inputData = (0 to 30).map(nr => (nr, nr*2, nr % 2 == 0)).toDF("id", "multiplication_result", "is_even")
      inputData.write.partitionBy("is_even").mode(saveMode).format("delta").save(outputDir)
    })

    val evenNumbers = sparkSession.read.format("delta").load(outputDir)
      .where("is_even = true")

    evenNumbers.explain(true)
    evenNumbers.show(false)
  }

}
