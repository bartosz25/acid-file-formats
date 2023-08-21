package com.waitingforcode

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils

import java.io.File
import java.util.concurrent.CountDownLatch

object App5AutomaticResolution {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(2)
    import sparkSession.implicits._
    val tableName = "app5"

    val inputData = (0 to 200000).map(nr => (nr, nr * 2, nr * 3, nr % 5))
      .toDF("id", "multiplication_result_x2", "multiplication_result_x3", "partition_number")
    inputData.write.format("delta")
      .saveAsTable(tableName)
    val deltaTable = DeltaTable.forName(tableName)

    sparkSession.read.table(tableName).select("id", "multiplication_result_x2",
        "partition_number").filter("id = 5 OR id = 10")
      .show(false)

    val barrier = new CountDownLatch(2)
    new Thread(() => {
      println(s"Starting 'inserts 1'")
      inputData.write.format("delta").insertInto(tableName)
      inputData.write.format("delta").insertInto(tableName)
      barrier.countDown()
    }).start()
    new Thread(() => {
      println(s"Starting 'inserts 2'")
      inputData.write.format("delta").insertInto(tableName)
      inputData.write.format("delta").insertInto(tableName)
      barrier.countDown()
    }).start()
    barrier.await()

    sparkSession.read.table(tableName).select("id", "multiplication_result_x2",
        "partition_number").filter("id = 5 OR id = 10")
      .show(false)
  }

}
