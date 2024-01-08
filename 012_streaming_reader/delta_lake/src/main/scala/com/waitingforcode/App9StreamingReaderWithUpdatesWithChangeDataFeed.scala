package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode

import java.io.File

/**
 * The demo should show duplicated rows streamed in case of updates.
 * Switch the "skipChangeCommits" option and rerun the demo to see the flag effect.
 */
object App9StreamingReaderWithUpdatesWithChangeDataFeed {

  def main(args: Array[String]): Unit = {
    val sparkSession = getDeltaLakeSparkSession(1)
    FileUtils.deleteDirectory(new File(outputDir))
    import sparkSession.implicits._
    val numbersWithLetters = Seq(
      (1, "a"), (2, "b"), (3, "c"), (4, "d")
    ).toDF("number", "letter")

    println("Generating data...")
    numbersWithLetters.write.format("delta").mode(SaveMode.Overwrite).saveAsTable(NumbersWithLettersTable)
    (2 to 3).foreach(nr => {
      sparkSession.sql(
        s"""
           |UPDATE ${NumbersWithLettersTable} SET letter = '${nr}-a'
           |WHERE number = 1
           |""".stripMargin)
      val numbersWithLettersToWrite = Seq(
        (nr*1, "a"), (nr*2, "b"), (nr*3, "c"), (nr*4, "d")
      ).toDF("number", "letter")
      numbersWithLettersToWrite.write.format("delta").insertInto(NumbersWithLettersTable)
    })
    println("...done, starting streaming data")

    sparkSession.readStream.format("delta")
      .option("startingVersion", 0)
      .option("skipChangeCommits", false)
      .option("readChangeFeed", true)
      .table(NumbersWithLettersTable)
      .writeStream.format("console")
      .option("checkpointLocation", s"${outputDir}/checkpoint9")
      .start()

    sparkSession.streams.awaitAnyTermination()


  }

}
