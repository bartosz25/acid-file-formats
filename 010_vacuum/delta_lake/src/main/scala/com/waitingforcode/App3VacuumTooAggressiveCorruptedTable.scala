package com.waitingforcode

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils

import java.io.File
import java.util.concurrent.CountDownLatch

/**
 * The job is supposed to fail after the last `SELECT`. However, it relies on the
 * concurrent code execution whose synchronisation is hard. That's why the failure might
 * not happen at every run.
 */
object App3VacuumTooAggressiveCorruptedTable {
  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(checkEnabled = false)
    import sparkSession.implicits._
    val tableName = "app3"

    val warehouseDir = s"${outputDir}/warehouse/$tableName"
    (100 to 105).toDF("id").writeTo(tableName).using("delta")
      .tableProperty("delta.deletedFileRetentionDuration", "interval 10 seconds")
      .createOrReplace()

    val currentFilesInDir = FileUtils.sizeOfDirectory(new File(warehouseDir))
    val countDownLatch = new CountDownLatch(1)
    new Thread(() => {
      (200 to 255).toDF("id").repartition(10).writeTo(tableName).append()
      (300 to 355).toDF("id").repartition(10).writeTo(tableName).append()
      (400 to 455).toDF("id").repartition(10).writeTo(tableName).append()
      countDownLatch.countDown()
    }).start()
    new Thread(() => {
      while (FileUtils.sizeOfDirectory(new File(warehouseDir)) == currentFilesInDir) {}
      println("New file created, starting the VACUUM")
      sparkSession.sql(
        s"""
           |VACUUM delta.`$warehouseDir` RETAIN 0 HOURS
           |""".stripMargin)
      countDownLatch.countDown()
    }).start()
    countDownLatch.await()

    DeltaTable.forName(tableName).history().show(false)
    // The history should print:
    // +-------+-----------------------+------+--------+---------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+----+--------+---------+-----------+-----------------+-------------+-------------------------------------------------------------+------------+-----------------------------------+
    //|version|timestamp              |userId|userName|operation                        |operationParameters                                                                                                                    |job |notebook|clusterId|readVersion|isolationLevel   |isBlindAppend|operationMetrics                                             |userMetadata|engineInfo                         |
    //+-------+-----------------------+------+--------+---------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+----+--------+---------+-----------+-----------------+-------------+-------------------------------------------------------------+------------+-----------------------------------+
    //|4      |2023-09-24 11:25:31.334|null  |null    |VACUUM START                     |{retentionCheckEnabled -> false, defaultRetentionMillis -> 10000, specifiedRetentionMillis -> 0}                                       |null|null    |null     |2          |SnapshotIsolation|true         |{numFilesToDelete -> 4, sizeOfDataToDelete -> 1884}          |null        |Apache-Spark/3.4.0 Delta-Lake/2.4.0|
    //|3      |2023-09-24 11:25:25.582|null  |null    |WRITE                            |{mode -> Append, partitionBy -> []}                                                                                                    |null|null    |null     |2          |Serializable     |true         |{numFiles -> 10, numOutputRows -> 56, numOutputBytes -> 4712}|null        |Apache-Spark/3.4.0 Delta-Lake/2.4.0|
    //|2      |2023-09-24 11:25:15.434|null  |null    |WRITE                            |{mode -> Append, partitionBy -> []}                                                                                                    |null|null    |null     |1          |Serializable     |true         |{numFiles -> 10, numOutputRows -> 56, numOutputBytes -> 4714}|null        |Apache-Spark/3.4.0 Delta-Lake/2.4.0|
    //|1      |2023-09-24 11:24:15.154|null  |null    |WRITE                            |{mode -> Append, partitionBy -> []}                                                                                                    |null|null    |null     |0          |Serializable     |true         |{numFiles -> 10, numOutputRows -> 56, numOutputBytes -> 4714}|null        |Apache-Spark/3.4.0 Delta-Lake/2.4.0|
    //|0      |2023-09-24 11:23:57.658|null  |null    |CREATE OR REPLACE TABLE AS SELECT|{isManaged -> true, description -> null, partitionBy -> [], properties -> {"delta.deletedFileRetentionDuration":"interval 10 seconds"}}|null|null    |null     |null       |Serializable     |false        |{numFiles -> 2, numOutputRows -> 6, numOutputBytes -> 922}   |null        |Apache-Spark/3.4.0 Delta-Lake/2.4.0|
    //+-------+-----------------------+------+--------+---------------------------------+---------------------------------------------------------------------------------------------------------------------------------------+----+--------+---------+-----------+-----------------+-------------+-------------------------------------------------------------+------------+-----------------------------------+
    sparkSession.sql(s"SELECT * FROM ${tableName}").show(false)
    // The select should fail with Exception in thread "main" org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 119.0 failed 1 times, most recent failure: Lost task 0.0 in stage 119.0 (TID 11578) (192.168.1.55 executor driver): org.apache.spark.SparkFileNotFoundException: File file:/tmp/table-file-formats/010_vacuum/delta_lake/warehouse/app3/part-00003-0dc1d7c4-9661-4df4-adcf-80522c4f9819-c000.snappy.parquet does not exist
    //It is possible the underlying files have been updated. You can explicitly invalidate the cache in Spark by running 'REFRESH TABLE tableName' command in SQL or by recreating the Dataset/DataFrame involved.
    //	at org.apache.spark.sql.errors.QueryExecutionErrors$.readCurrentFileNotFoundError(QueryExecutionErrors.scala:794)
    //	at org.apache.spark.sql.execution.datasources.FileScanRDD$$anon$1.org$apache$spark$sql$execution$datasources$FileScanRDD$$anon$$readCurrentFile(FileScanRDD.scala:231)
    //	at org.apache.spark.sql.execution.datasources.FileScanRDD$$anon$1.nextIterator(FileScanRDD.scala:290)
    //	at org.apache.spark.sql.execution.datasources.FileScanRDD$$anon$1.hasNext(FileScanRDD.scala:125)
    //	at org.apache.spark.sql.execution.FileSourceScanExec$$anon$1.hasNext(DataSourceScanExec.scala:594)
    // if the VACUUM was a concurrent action to the appends
  }



}
