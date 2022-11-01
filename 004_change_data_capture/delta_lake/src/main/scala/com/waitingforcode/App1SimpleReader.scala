package com.waitingforcode

import org.apache.commons.io.FileUtils

import java.io.File
import java.time.{LocalDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit

object App1SimpleReader {

  def main(args: Array[String]): Unit = {
    val sparkSession = getDeltaLakeSparkSession(1)
    FileUtils.deleteDirectory(new File(outputDir))
    val tableName = "rateData"

    val rateStreamInput = sparkSession.readStream.format("rate-micro-batch")
      .option("rowsPerBatch", 5)
      .option("numPartitions", 2)
      .option("advanceMillisPerMicroBatch", TimeUnit.MINUTES.toMillis(2))
      .option("startTimestamp", LocalDateTime.of(2022, 5, 25, 10, 25).toInstant(ZoneOffset.UTC).toEpochMilli)
      .load()
    val dataGenerationQuery = rateStreamInput.writeStream
      .option("checkpointLocation", s"${outputDir}/checkpoint")
      .format("delta").toTable(tableName)

    val cdcQuery = sparkSession.readStream.format("delta").option("readChangeFeed", "true")
      .option("startingVersion", 0).table(tableName)
      .writeStream.format("console").start()

    sparkSession.streams.awaitAnyTermination()
  }

}
