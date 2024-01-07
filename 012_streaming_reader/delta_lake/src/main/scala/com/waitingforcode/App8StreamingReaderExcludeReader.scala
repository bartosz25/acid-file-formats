package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.io.File

object App8StreamingReaderExcludeReader {

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

    println("Generating data....")
    getNumbers(1).write.format("delta").mode(SaveMode.Overwrite).saveAsTable(NumbersWithLettersTable)
    getNumbers(2).write.format("delta").insertInto(NumbersWithLettersTable)
    getNumbers(3).write.format("delta").insertInto(NumbersWithLettersTable)
    getNumbers(4).write.format("delta").insertInto(NumbersWithLettersTable)
    getNumbers(5).write.format("delta").insertInto(NumbersWithLettersTable)
    getNumbers(6).write.format("delta").insertInto(NumbersWithLettersTable)
    println("...data generated, starting streaming reader")

    sparkSession.readStream.format("delta")
      .option("startingVersion", 0)
      .option("excludeRegex", "part-000") // Exclude all files
      .table(NumbersWithLettersTable)
      .writeStream.format("console").start()

    sparkSession.streams.awaitAnyTermination()
  }

}
