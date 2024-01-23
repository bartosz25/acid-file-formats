package com.waitingforcode

import com.waitingforcode.App1IdempotentWriterRun1.{AppId, CheckpointLocation, Table1Location, Table2Location}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.DataFrame

import java.io.File
import java.time.{LocalDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit

/**
 * Run App1IdempotentWriterRun1 first. It should fail with a RuntimeException.
 * After the failure, run App1IdempotentWriterRun2. It'll retry the failed micro-batch
 * but only for the table 2. As a result, you shouldn't see any duplicates in the queries.
 *
 * To see the opposite, remove the
 * `Map("txnVersion" -> batchVersion.toString, "txnAppId" -> AppId)` options or
 * change the `AppId` between the runs.
 */
object App1IdempotentWriterRun1 {

  val AppId = "wfc-v1"

  val CheckpointLocation = s"${outputDir}/app1/checkpoint"
  val Table1Location = s"${outputDir}/app1/table1"
  val Table2Location = s"${outputDir}/app1/table2"

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))
    val sparkSession = getDeltaLakeSparkSession(3)

    val rateStreamInput = sparkSession.readStream.format("rate-micro-batch")
      .option("rowsPerBatch", 500)
      .option("numPartitions", 10)
      .option("advanceMillisPerMicroBatch", TimeUnit.MINUTES.toMillis(2))
      .option("startTimestamp", LocalDateTime.of(2022, 5, 25, 10, 25).toInstant(ZoneOffset.UTC).toEpochMilli)
      .load()

    rateStreamInput.writeStream.option("checkpointLocation", CheckpointLocation)
      .format("delta").foreachBatch((dataset: DataFrame, batchVersion: Long) => {
        dataset.write.format("delta").mode("append").options(Map(
          "txnVersion" -> batchVersion.toString, "txnAppId" -> AppId
        )).save(Table1Location)

        throw new RuntimeException("An error occurred before writing table 2")

        dataset.write.format("delta").mode("append").options(Map(
          "txnVersion" -> batchVersion.toString, "txnAppId" -> AppId
        )).save(Table2Location)
        ()
      }).start()

    sparkSession.streams.awaitAnyTermination()
  }

}

object App1IdempotentWriterRun2 {
  def main(args: Array[String]): Unit = {
    val sparkSession = getDeltaLakeSparkSession(3)
    val rateStreamInput = sparkSession.readStream.format("rate-micro-batch")
      .option("rowsPerBatch", 500)
      .option("numPartitions", 10)
      .option("advanceMillisPerMicroBatch", TimeUnit.MINUTES.toMillis(2))
      .option("startTimestamp", LocalDateTime.of(2022, 5, 25, 10, 25).toInstant(ZoneOffset.UTC).toEpochMilli)
      .load()

    rateStreamInput.writeStream.option("checkpointLocation", CheckpointLocation)
      .format("delta").foreachBatch((dataset: DataFrame, batchVersion: Long) => {
        dataset.write.format("delta").mode("append").options(Map(
          "txnVersion" -> batchVersion.toString, "txnAppId" -> AppId
        )).save(Table1Location)

        dataset.write.format("delta").mode("append").options(Map(
          "txnVersion" -> batchVersion.toString, "txnAppId" -> AppId
        )).save(Table2Location)
        ()
      }).start()

    sparkSession.streams.awaitAnyTermination(45000L)

    import sparkSession.implicits._
    sparkSession.read.format("delta").load(Table1Location).groupBy($"timestamp", $"value")
      .count().filter("count > 1").show(false)
    sparkSession.read.format("delta").load(Table2Location).groupBy($"timestamp", $"value")
      .count().filter("count > 1").show(false)

  }
}