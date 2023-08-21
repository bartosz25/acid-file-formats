package com.waitingforcode

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils

import java.io.File
import java.util.concurrent.CountDownLatch

object App3NoConflictUpdateDifferentPartition {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(2)
    import sparkSession.implicits._
    val tableName = "app2"

    val inputData = (0 to 200000).map(nr => (nr, nr * 2, nr * 3, nr % 5))
      .toDF("id", "multiplication_result_x2", "multiplication_result_x3", "partition_number")
    inputData.write.partitionBy("partition_number").format("delta")
      .saveAsTable(tableName)
    val deltaTable = DeltaTable.forName(tableName)

    sparkSession.read.table(tableName).select("id", "multiplication_result_x2",
        "partition_number").filter("id = 5 OR id = 4")
      .show(false)

    val barrier = new CountDownLatch(2)
    new Thread(() => {
      println(s"Starting 'id = 5 AND partition_number = 0'")
      deltaTable.updateExpr("id = 5 AND partition_number = 0", Map("multiplication_result_x2" -> "-1"))
      deltaTable.updateExpr("id = 5 AND partition_number = 0", Map("multiplication_result_x2" -> "-1"))
      deltaTable.updateExpr("id = 5 AND partition_number = 0", Map("multiplication_result_x2" -> "-1"))
      barrier.countDown()
    }).start()
    new Thread(() => {
      println(s"Starting 'id = 4 AND partition_number = 4'")
      deltaTable.updateExpr("id = 4 AND partition_number = 4", Map("multiplication_result_x2" -> "-100"))
      deltaTable.updateExpr("id = 4 AND partition_number = 4", Map("multiplication_result_x2" -> "-100"))
      deltaTable.updateExpr("id = 4 AND partition_number = 4", Map("multiplication_result_x2" -> "-100"))
      barrier.countDown()
    }).start()
    barrier.await()

    sparkSession.read.table(tableName).select("id", "multiplication_result_x2",
        "partition_number").filter("id = 5 OR id = 4")
      .show(false)
  }

}
