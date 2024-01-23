package com.waitingforcode

import com.waitingforcode.App2SchemaConflictsWriterRun1.TableName
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions

import java.io.File
import java.time.{LocalDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit

/**
 * Start App2SchemaConflictsWriterRun1 first and stop after running a few micro-batches.
 *
 * Run the App2SchemaConflictsWriterRun2. This job adds a new column and doesn't set any
 * schema conflict management flag. It should lead to the following error:
 *
 * > For other operations, set the session configuration
    spark.databricks.delta.schema.autoMerge.enabled to "true". See the documentation
    specific to the operation for details.

    Table schema:
    root
    -- timestamp: timestamp (nullable = true)
    -- value: long (nullable = true)


    Data schema:
    root
    -- timestamp: timestamp (nullable = true)
    -- value: long (nullable = true)
    -- a: string (nullable = true)
 *
 * Uncomment the `` and restart the App2SchemaConflictsWriterRun2.
 */
object App2SchemaConflictsWriterRun1 {

  val CheckpointLocation = s"${outputDir}/app2/checkpoint"
  val TableName ="app2_table"

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
      .format("delta").toTable(TableName)

    sparkSession.streams.awaitAnyTermination()
  }

}

object App2SchemaConflictsWriterRun2 {
  def main(args: Array[String]): Unit = {
    val sparkSession = getDeltaLakeSparkSession(3)
    val rateStreamInput = sparkSession.readStream.format("rate-micro-batch")
      .option("rowsPerBatch", 500)
      .option("numPartitions", 10)
      .option("advanceMillisPerMicroBatch", TimeUnit.MINUTES.toMillis(2))
      .option("startTimestamp", LocalDateTime.of(2022, 5, 25, 10, 25).toInstant(ZoneOffset.UTC).toEpochMilli)
      .load()

    val rateStreamInputWithNewColumn = rateStreamInput.withColumn("a", functions.lit("a"))

    rateStreamInputWithNewColumn.writeStream
      .option("checkpointLocation", App2SchemaConflictsWriterRun1.CheckpointLocation)
      .option("mergeSchema", true)
      .format("delta").toTable(TableName)

    sparkSession.streams.awaitAnyTermination()
  }
}