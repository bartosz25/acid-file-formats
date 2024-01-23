package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions
import org.apache.spark.sql.streaming.OutputMode

import java.io.File
import java.time.{LocalDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit

/**
 * Start App3SchemaOverwriteWriterRun1 first and stop after running a few micro-batches.
 *
 * Run the App3SchemaOverwriteWriterRun2. This job makes an incompatible schema change so the
 * single way to go is the `overwriteSchema` option. However, it runs only with the "complete" output
 * mode. Therefore, the first run should fail with:
 * >  Exception in thread "main" org.apache.spark.sql.streaming.StreamingQueryException: Failed to merge fields 'value' and 'value'. Failed to merge incompatible data types LongType and ArrayType(StringType,true)
      === Streaming Query ===
      Identifier: [id = 235f3216-c0c9-49bc-b6bf-2d65c7ef6146, runId = 85b70014-2431-41a2-8ec1-36a5b350340c]
      Current Committed Offsets: {RatePerMicroBatchStream[rowsPerBatch=500, numPartitions=10, startTimestamp=1653474300000, advanceMsPerBatch=1000]: {"offset":2500,"timestamp":1653474305000}}
      Current Available Offsets: {RatePerMicroBatchStream[rowsPerBatch=500, numPartitions=10, startTimestamp=1653474300000, advanceMsPerBatch=1000]: {"offset":3000,"timestamp":1653474306000}}

      Current State: ACTIVE
      Thread State: RUNNABLE

      Logical Plan:
      WriteToMicroBatchDataSourceV1 `spark_catalog`.`default`.`app3_table`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, DeltaSink[file:/tmp/table-file-formats/013_streaming_writer/delta_lake/warehouse/app3_table], 235f3216-c0c9-49bc-b6bf-2d65c7ef6146, [checkpointLocation=/tmp/table-file-formats/013_streaming_writer/delta_lake/app3/checkpoint, overwriteSchema=true, path=file:/tmp/table-file-formats/013_streaming_writer/delta_lake/warehouse/app3_table], Append
      +- Project [timestamp#0, array(a, b) AS value#4]
         +- StreamingDataSourceV2Relation [timestamp#0, value#1L], org.apache.spark.sql.execution.streaming.sources.RatePerMicroBatchTable$$anon$1@16efbf65, RatePerMicroBatchStream[rowsPerBatch=500, numPartitions=10, startTimestamp=1653474300000, advanceMsPerBatch=1000]
 *
 * Uncomment the "complete" mode and restart the App3SchemaOverwriteWriterRun2. However, the job
 * still fails:
 *
 * > Exception in thread "main" org.apache.spark.sql.AnalysisException: Complete output mode not supported when there are no streaming aggregations on streaming DataFrames/Datasets;
    Project [timestamp#0, array(a, b) AS value#4]
    +- StreamingRelationV2 org.apache.spark.sql.execution.streaming.sources.RatePerMicroBatchProvider@3ea9a091, rate-micro-batch, org.apache.spark.sql.execution.streaming.sources.RatePerMicroBatchTable@45c408a4, [rowsPerBatch=500, advanceMillisPerMicroBatch=120000, startTimestamp=1653474300000, numPartitions=10], [timestamp#0, value#1L]
 */
object App3SchemaOverwriteWriterRun1 {

  val CheckpointLocation = s"${outputDir}/app3/checkpoint"
  val TableName ="app3_table"

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

object App3SchemaOverwriteWriterRun2 {
  def main(args: Array[String]): Unit = {
    val sparkSession = getDeltaLakeSparkSession(3)
    val rateStreamInput = sparkSession.readStream.format("rate-micro-batch")
      .option("rowsPerBatch", 500)
      .option("numPartitions", 10)
      .option("advanceMillisPerMicroBatch", TimeUnit.MINUTES.toMillis(2))
      .option("startTimestamp", LocalDateTime.of(2022, 5, 25, 10, 25).toInstant(ZoneOffset.UTC).toEpochMilli)
      .load()

    val rateStreamInputWithNewColumn = rateStreamInput.withColumn("value",
      functions.array(functions.lit("a"), functions.lit("b")))

    rateStreamInputWithNewColumn.writeStream
      .option("checkpointLocation", App3SchemaOverwriteWriterRun1.CheckpointLocation)
      .option("overwriteSchema", true)
      .outputMode(OutputMode.Append())
      //.outputMode(OutputMode.Complete())
      .format("delta").toTable(App3SchemaOverwriteWriterRun1.TableName)

    sparkSession.streams.awaitAnyTermination()
  }
}