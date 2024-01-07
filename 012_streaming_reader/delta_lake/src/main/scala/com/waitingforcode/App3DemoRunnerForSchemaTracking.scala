package com.waitingforcode

/**
 * Demo scenario:
 * 1. Run `App3DemoRunnerForSchemaTracking`
 * 2. It should fail with:
 * ```
Caused by: org.apache.spark.sql.delta.DeltaRuntimeException: [DELTA_STREAMING_METADATA_EVOLUTION] The schema, table configuration or protocol of your Delta table has changed during streaming.
The schema or metadata tracking log has been updated.
Please restart the stream to continue processing using the updated metadata.
Updated schema: root
 |-- id_number: integer (nullable = true)
 |-- letter: string (nullable = true)
.
Updated table configurations: delta.enableChangeDataFeed:true, delta.columnMapping.mode:name, delta.columnMapping.maxColumnId:2.
Updated table protocol: 2,5
 * ```
 * 3. Restart the `App3StreamingReader`. It should fail with:
 ```
Exception in thread "main" org.apache.spark.sql.streaming.StreamingQueryException: [DELTA_STREAMING_CANNOT_CONTINUE_PROCESSING_POST_SCHEMA_EVOLUTION] We've detected one or more non-additive schema change(s) (RENAME COLUMN) between Delta version 1 and 2 in the Delta streaming source.
Please check if you want to manually propagate the schema change(s) to the sink table before we proceed with stream processing using the finalized schema at 2.
Once you have fixed the schema of the sink table or have decided there is no need to fix, you can set (one of) the following SQL configurations to unblock the non-additive schema change(s) and continue stream processing.
To unblock for this particular stream just for this series of schema change(s): set `spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop.ckpt_-2054049119 = 2`.
To unblock for this particular stream: set `spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop.ckpt_-2054049119 = always`
To unblock for all streams: set `spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop = always`.
Alternatively if applicable, you may replace the `allowSourceColumnRenameAndDrop` with `allowSourceColumnRename` in the SQL conf to unblock stream for just this schema change type.
```
 * 4. Uncomment the line:
 * ```
 * //"spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop" -> "always"
 * ```
 * in the App3StreamingReader
 * 4. Start the `App3StreamingReader`. It should process data correctly.
 */
object App3DemoRunnerForSchemaTracking {

  def main(args: Array[String]): Unit = {
    println("Creating table...")
    App3BatchWriter1.main(args)
    println("...table created")
    new Thread(() => {
      println("Starting streaming reader")
      App3StreamingReader.main(args)
    }).start()

    while(!App3StreamingReader.StreamStarted.get()) {}

    println("Renaming an existing column...")
    App3BatchWriter3RenameStreamedColumnWithSchemaOverwrite.main(args)
    println("...column renamed")
  }
}
