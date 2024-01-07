package com.waitingforcode

/**
 * 1. Run the `App2DemoRunner`. It should fail with:
 ```
Exception in thread "Thread-33" org.apache.spark.sql.streaming.StreamingQueryException: [DELTA_SCHEMA_CHANGED_WITH_VERSION] Detected schema change in version 1:
streaming source schema: root
-- number: integer (nullable = true)
-- letter: string (nullable = true)


data file schema: root
-- number: integer (nullable = true)
-- letter: string (nullable = true)
-- upper_letter: string (nullable = true)


Please try restarting the query. If this issue repeats across query restarts without
making progress, you have made an incompatible schema change and need to start your
query from scratch using a new checkpoint directory.
 ```
 2. Restart the `App2StreamingReader`. It should include the new column now.
 */
object App2DemoRunnerForNewColumn {

  def main(args: Array[String]): Unit = {
    println("Creating table...")
    App2BatchWriter1.main(args)
    println("...table created")
    new Thread(() => {
      println("Starting streaming reader")
      App2StreamingReader.main(args)
    }).start()
    while(!App2StreamingReader.StreamStarted.get()) {}
    println("Adding new column...")
    App2BatchWriter2AddNewColumns.main(args)
    println("...new column added")
  }
}
