package com.waitingforcode

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.io.File

object App6StreamingReaderFromTimeTravelAfterSchemaChanges {

  def main(args: Array[String]): Unit = {
    val sparkSession = getDeltaLakeSparkSession(1)
    FileUtils.deleteDirectory(new File(outputDir))
    import sparkSession.implicits._
    def getNumbers(prefix: Int, indexColumn: String = "number"): DataFrame = {
      val numbersWithLetters = Seq(
        (prefix*1, "a"), (prefix*2, "b"), (prefix*3, "c"), (prefix*4, "d")
      ).toDF(indexColumn, "letter")
      numbersWithLetters
    }

    getNumbers(1).write.format("delta").mode(SaveMode.Overwrite).saveAsTable(NumbersWithLettersTable)
    sparkSession.sql(
      s"""
         |  ALTER TABLE ${NumbersWithLettersTable} SET TBLPROPERTIES (
         |    'delta.minReaderVersion' = '2',
         |    'delta.minWriterVersion' = '5',
         |    'delta.columnMapping.mode' = 'name'
         |  )
         |""".stripMargin)
    getNumbers(2).write.format("delta").insertInto(NumbersWithLettersTable)
    getNumbers(3).write.format("delta").insertInto(NumbersWithLettersTable)
    getNumbers(4).write.format("delta").insertInto(NumbersWithLettersTable)
    sparkSession.sql(s"ALTER TABLE ${NumbersWithLettersTable} RENAME COLUMN number TO id_number")
    getNumbers(5, "id_number").write.format("delta").insertInto(NumbersWithLettersTable)

    DeltaTable.forName(NumbersWithLettersTable).history().show(truncate = false)

    val checkpointLocation = s"${outputDir}/checkpoint6"
    sparkSession.readStream.format("delta")
      .option("startingVersion", 2) // multiplier of 3
      .option("schemaTrackingLocation", s"${checkpointLocation}/schema")
      .table(NumbersWithLettersTable)
      .writeStream.format("console").option("checkpointLocation", checkpointLocation).start()

    sparkSession.streams.awaitAnyTermination()
  }

}
