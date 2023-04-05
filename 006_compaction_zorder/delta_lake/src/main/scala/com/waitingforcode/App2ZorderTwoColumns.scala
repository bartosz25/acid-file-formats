package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.delta.DeltaLog

import java.io.File

object App2ZorderTwoColumns {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(2)

    import sparkSession.implicits._
    val inputData = (0 to 20).map(nr => (nr, nr*2, nr * 4)).toDF("id1", "id2", "multiplication_result")
    inputData.write.mode(SaveMode.Overwrite).format("delta").save(outputDir)

    val deltaLog = DeltaLog.forTable(sparkSession, outputDir)

    sparkSession.sql(s"""OPTIMIZE '${outputDir}' ZORDER BY id1, id2""")
  }

}
