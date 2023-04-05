package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode

import java.io.File

object App6SchemaChangeDMLForNotNullColumns {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(2)

    import sparkSession.implicits._
    val inputData = (0 to 2).map(nr => (nr, nr * 2)).toDF("id", "multiplication_result")
    inputData.write.mode(SaveMode.Overwrite).format("delta").save(outputDir)

    sparkSession.sql(s"CREATE TABLE default.numbers USING DELTA LOCATION '${outputDir}'")

    sparkSession.sql("ALTER TABLE default.numbers ADD COLUMNS division_result INT NOT NULL")
  }

}
