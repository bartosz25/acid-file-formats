package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode

import java.io.File

object App7SchemaChangeTypeBytesToInt {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(2)

    import sparkSession.implicits._
    val inputData = (0 to 2).map(nr => (nr, nr.toByte)).toDF("id", "id_as_byte")
    inputData.write.mode(SaveMode.Overwrite).format("delta").save(outputDir)

    val inputDataWithDivisionResult = (4 to 6).map(nr => (nr, nr)).toDF("id", "id_as_byte")
    inputDataWithDivisionResult.write.option("mergeSchema", "true").mode(SaveMode.Append).format("delta").save(outputDir)

    sparkSession.read.format("delta").load(outputDir).show(false)
  }

}
