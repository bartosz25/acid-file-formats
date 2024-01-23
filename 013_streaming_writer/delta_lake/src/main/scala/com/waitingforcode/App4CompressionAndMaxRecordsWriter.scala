package com.waitingforcode

import com.waitingforcode.App4CompressionAndMaxRecordsWriter.TableName
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.delta.DeltaLog

import java.io.File
import java.time.{LocalDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit

/**
 * Run the App4CompressionAndMaxRecordsWriter and stop after completing few micro-batches.
 *
 * Run the App4CheckDeltaLog. You should see that each file contains one record:
 * >
    +---------------------------------------------------------------+---------------+----+----------------+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----+--------------+---------+-----------------------+
    |path                                                           |partitionValues|size|modificationTime|dataChange|stats                                                                                                                                                                                             |tags|deletionVector|baseRowId|defaultRowCommitVersion|
    +---------------------------------------------------------------+---------------+----+----------------+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----+--------------+---------+-----------------------+
    |part-00000-e6165041-6a95-4ccc-b7fa-d5d5868856bc-c006.gz.parquet|{}             |826 |1705994421022   |false     |{"numRecords":1,"minValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":60},"maxValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":60},"nullCount":{"timestamp":0,"value":0}}  |NULL|NULL          |NULL     |NULL                   |
    |part-00001-b1ff6cf9-f830-432e-a48f-89e4419b88d4-c030.gz.parquet|{}             |827 |1705994427250   |false     |{"numRecords":1,"minValues":{"timestamp":"2022-05-25T12:25:01.000+02:00","value":801},"maxValues":{"timestamp":"2022-05-25T12:25:01.000+02:00","value":801},"nullCount":{"timestamp":0,"value":0}}|NULL|NULL          |NULL     |NULL                   |
    |part-00001-b88b4729-c60c-4dab-8558-5ff679cc2ee3-c041.gz.parquet|{}             |827 |1705994421442   |false     |{"numRecords":1,"minValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":411},"maxValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":411},"nullCount":{"timestamp":0,"value":0}}|NULL|NULL          |NULL     |NULL                   |
    |part-00003-41fc5092-b09e-465d-bbce-e2bb01385b00-c002.gz.parquet|{}             |827 |1705994427522   |false     |{"numRecords":1,"minValues":{"timestamp":"2022-05-25T12:25:01.000+02:00","value":523},"maxValues":{"timestamp":"2022-05-25T12:25:01.000+02:00","value":523},"nullCount":{"timestamp":0,"value":0}}|NULL|NULL          |NULL     |NULL                   |
    |part-00003-7909d0fe-176d-4777-8720-89fc03b91f62-c019.gz.parquet|{}             |825 |1705994421870   |false     |{"numRecords":1,"minValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":193},"maxValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":193},"nullCount":{"timestamp":0,"value":0}}|NULL|NULL          |NULL     |NULL                   |
    |part-00003-80e3a2f3-9ead-4ed5-8f9a-eb39896f01fc-c024.gz.parquet|{}             |826 |1705994421950   |false     |{"numRecords":1,"minValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":243},"maxValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":243},"nullCount":{"timestamp":0,"value":0}}|NULL|NULL          |NULL     |NULL                   |
    |part-00004-33b7b499-c7d4-4b33-a58d-11720e1b5420-c044.gz.parquet|{}             |827 |1705994427886   |false     |{"numRecords":1,"minValues":{"timestamp":"2022-05-25T12:25:01.000+02:00","value":944},"maxValues":{"timestamp":"2022-05-25T12:25:01.000+02:00","value":944},"nullCount":{"timestamp":0,"value":0}}|NULL|NULL          |NULL     |NULL                   |
    |part-00004-39e3bf87-7c69-4e19-a1da-0f20e8646200-c044.gz.parquet|{}             |827 |1705994422270   |false     |{"numRecords":1,"minValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":444},"maxValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":444},"nullCount":{"timestamp":0,"value":0}}|NULL|NULL          |NULL     |NULL                   |
    |part-00005-7f2ae03c-3dec-4778-b4c7-9794e003f11b-c047.gz.parquet|{}             |827 |1705994427918   |false     |{"numRecords":1,"minValues":{"timestamp":"2022-05-25T12:25:01.000+02:00","value":975},"maxValues":{"timestamp":"2022-05-25T12:25:01.000+02:00","value":975},"nullCount":{"timestamp":0,"value":0}}|NULL|NULL          |NULL     |NULL                   |
    |part-00006-9d6f4a9f-079c-434a-8353-f181cd8a6038-c012.gz.parquet|{}             |827 |1705994428042   |false     |{"numRecords":1,"minValues":{"timestamp":"2022-05-25T12:25:01.000+02:00","value":626},"maxValues":{"timestamp":"2022-05-25T12:25:01.000+02:00","value":626},"nullCount":{"timestamp":0,"value":0}}|NULL|NULL          |NULL     |NULL                   |
    |part-00006-b4d11438-d48c-45fb-a7d6-2a040efbe25b-c013.gz.parquet|{}             |826 |1705994422542   |false     |{"numRecords":1,"minValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":136},"maxValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":136},"nullCount":{"timestamp":0,"value":0}}|NULL|NULL          |NULL     |NULL                   |
    |part-00007-3ffbde69-41a3-4e74-bfd2-b57b11bd0337-c010.gz.parquet|{}             |826 |1705994422486   |false     |{"numRecords":1,"minValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":107},"maxValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":107},"nullCount":{"timestamp":0,"value":0}}|NULL|NULL          |NULL     |NULL                   |
    |part-00007-bbfd7274-3852-4f2d-856f-fddc648aa05e-c040.gz.parquet|{}             |827 |1705994422870   |false     |{"numRecords":1,"minValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":407},"maxValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":407},"nullCount":{"timestamp":0,"value":0}}|NULL|NULL          |NULL     |NULL                   |
    |part-00007-ed02729b-dce8-43a6-a206-a3bd4dae516e-c015.gz.parquet|{}             |826 |1705994422594   |false     |{"numRecords":1,"minValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":157},"maxValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":157},"nullCount":{"timestamp":0,"value":0}}|NULL|NULL          |NULL     |NULL                   |
    |part-00008-65b73bf9-8465-445b-82a6-4eb393ebd822-c040.gz.parquet|{}             |827 |1705994422922   |false     |{"numRecords":1,"minValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":408},"maxValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":408},"nullCount":{"timestamp":0,"value":0}}|NULL|NULL          |NULL     |NULL                   |
    |part-00008-bd56c253-c4f6-4d09-9047-4f729041aae7-c020.gz.parquet|{}             |826 |1705994422690   |false     |{"numRecords":1,"minValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":208},"maxValues":{"timestamp":"2022-05-25T12:25:00.000+02:00","value":208},"nullCount":{"timestamp":0,"value":0}}|NULL|NULL          |NULL     |NULL                   |
    |part-00000-985d5d52-dfad-440f-af36-a6c342afc9d8-c040.gz.parquet|{}             |827 |1705994427386   |false     |{"numRecords":1,"minValues":{"timestamp":"2022-05-25T12:25:01.000+02:00","value":900},"maxValues":{"timestamp":"2022-05-25T12:25:01.000+02:00","value":900},"nullCount":{"timestamp":0,"value":0}}|NULL|NULL          |NULL     |NULL                   |
    |part-00000-a86c9e84-767d-486f-9410-9b8a59595534-c037.gz.parquet|{}             |827 |1705994427338   |false     |{"numRecords":1,"minValues":{"timestamp":"2022-05-25T12:25:01.000+02:00","value":870},"maxValues":{"timestamp":"2022-05-25T12:25:01.000+02:00","value":870},"nullCount":{"timestamp":0,"value":0}}|NULL|NULL          |NULL     |NULL                   |
    |part-00001-43277b78-c67f-4609-a988-c4317cced9f1-c021.gz.parquet|{}             |827 |1705994427182   |false     |{"numRecords":1,"minValues":{"timestamp":"2022-05-25T12:25:01.000+02:00","value":711},"maxValues":{"timestamp":"2022-05-25T12:25:01.000+02:00","value":711},"nullCount":{"timestamp":0,"value":0}}|NULL|NULL          |NULL     |NULL                   |
    |part-00001-ce686d28-db6c-4348-aa4b-5dbc0c5b06c8-c032.gz.parquet|{}             |827 |1705994427294   |false     |{"numRecords":1,"minValues":{"timestamp":"2022-05-25T12:25:01.000+02:00","value":821},"maxValues":{"timestamp":"2022-05-25T12:25:01.000+02:00","value":821},"nullCount":{"timestamp":0,"value":0}}|NULL|NULL          |NULL     |NULL                   |
    +---------------------------------------------------------------+---------------+----+----------------+----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----+--------------+---------+-----------------------+
    only showing top 20 rows
    All files=1000
 *
 *
 */
object App4CompressionAndMaxRecordsWriter {

  val CheckpointLocation = s"${outputDir}/app4/checkpoint"
  val TableName ="app4_table"

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
      .option("maxRecordsPerFile", 1)
      .option("compression", "gzip")
      .format("delta").toTable(TableName)

    sparkSession.streams.awaitAnyTermination()
  }

}

object App4CheckDeltaLog {

  def main(args: Array[String]): Unit = {
    val sparkSession = getDeltaLakeSparkSession(3)

    val deltaLog = DeltaLog.forTable(sparkSession, s"${dataWarehouseBaseDir}/${TableName}")

    deltaLog.unsafeVolatileSnapshot.allFiles.show(false)

    println("All files="+deltaLog.unsafeVolatileSnapshot.allFiles.count())
  }
}
