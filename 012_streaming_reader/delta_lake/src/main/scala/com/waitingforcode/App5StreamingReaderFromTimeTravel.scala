package com.waitingforcode

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.io.File

object App5StreamingReaderFromTimeTravel {

  def main(args: Array[String]): Unit = {
    val sparkSession = getDeltaLakeSparkSession(1)
    FileUtils.deleteDirectory(new File(outputDir))
    import sparkSession.implicits._
    def getNumbers(prefix: Int): DataFrame = {
      val numbersWithLetters = Seq(
        (prefix*1, "a"), (prefix*2, "b"), (prefix*3, "c"), (prefix*4, "d")
      ).toDF("number", "letter")
      numbersWithLetters
    }

    getNumbers(1).write.format("delta").mode(SaveMode.Overwrite).saveAsTable(NumbersWithLettersTable)
    getNumbers(2).write.format("delta").insertInto(NumbersWithLettersTable)
    getNumbers(3).write.format("delta").insertInto(NumbersWithLettersTable)
    getNumbers(4).write.format("delta").insertInto(NumbersWithLettersTable)

    DeltaTable.forName(NumbersWithLettersTable).history().show(truncate = false)

    sparkSession.readStream.format("delta")
      .option("startingVersion", 2) // multiplier of 3
      .table(NumbersWithLettersTable)
      .writeStream.format("console").start()

    sparkSession.streams.awaitAnyTermination()
  }

}
