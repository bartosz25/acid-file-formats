package com.waitingforcode

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils

import java.io.File
import java.util.concurrent.CountDownLatch

object App3IsolationLevelDowngrade {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(2)
    import sparkSession.implicits._
    val tableName = "app3"
    val tableDir = s"/${outputDir}/${tableName}"

    sparkSession.sql(
      s"""
        |CREATE TABLE ${tableName} (id bigint) USING delta
        | TBLPROPERTIES ('delta.isolationLevel' = 'Serializable')
        |""".stripMargin)

    (0 to 100).toDF("id").writeTo(tableName).using("delta").createOrReplace()
    val deltaTable = DeltaTable.forName(tableName)

    val barrier = new CountDownLatch(2)
    new Thread(() => {
      try {
        println("Starting the 'OPTIMIZE'")
        deltaTable.optimize().executeCompaction()
      } finally {
        barrier.countDown()
      }
    }).start()
    new Thread(() => {
      try {
        while (barrier.getCount != 1) {}
        // Soon after the OPTIMIZE, let's start the writing
        println("Starting the 'update 100'")
        (0 to 100).toDF("id").write.format("delta").insertInto(tableName)
      } finally {
        barrier.countDown()
      }
    }).start()
    barrier.await()

    DeltaTable.forName(tableName).toDF.show(false)
  }

}
