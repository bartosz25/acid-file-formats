package com.waitingforcode

/**
 * Demo scenario:
 * 1. Run `App4DemoRunnerForUnsafeConfig`
 * 2. It should fail with:
 * ```
 * Caused by: org.apache.spark.sql.delta.DeltaIllegalStateException: [DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE] The streaming query was reading from an unexpected Delta table (id = '49701ff4-c65f-4174-a6da-9e1bcf238f7c').
    It used to read from another Delta table (id = '11b8ce48-6e0e-414c-a980-4c474d18baa3') according to checkpoint.
    This may happen when you changed the code to read from a new table or you deleted and
    re-created a table. Please revert your change or delete your streaming query checkpoint
    to restart from scratch.
 * ```
 * 3. Uncomment the
 * ```
 * //"spark.databricks.delta.streaming.unsafeReadOnIncompatibleColumnMappingSchemaChanges.enabled" -> true,
 * ```
 * in the App4StreamingReader
 * 4. Start the `App4StreamingReader`. It should process data correctly.
 */
object App4DemoRunnerForUnsafeConfig {

  def main(args: Array[String]): Unit = {
    println("Creating table...")
    App4BatchWriter1.main(args)
    println("...table created")
    new Thread(() => {
      println("Starting streaming reader")
      App4StreamingReader.main(args)
    }).start()

    while(!App4StreamingReader.StreamStarted.get()) {}

    println("Renaming an existing column...")
    App4BatchWriter3RenameStreamedColumnWithSchemaOverwrite.main(args)
    println("...column renamed")
  }
}
