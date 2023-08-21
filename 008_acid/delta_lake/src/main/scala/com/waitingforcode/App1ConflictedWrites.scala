package com.waitingforcode

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils

import java.io.File
import java.util.concurrent.CountDownLatch

object App1ConflictedWrites {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(2)
    import sparkSession.implicits._
    val tableName = "app1"

    val inputData = (0 to 2).map(nr => (nr, nr * 2, nr * 3))
      .toDF("id", "multiplication_result_x2", "multiplication_result_x3")
    inputData.write.format("delta").saveAsTable(tableName)
    val deltaTable = DeltaTable.forName(tableName)

    val barrier = new CountDownLatch(2)
    new Thread(() => {
      try {
        println("Starting the 'update -1'")
        deltaTable.updateExpr("id = 0", Map("multiplication_result_x2" -> "-1"))
        deltaTable.updateExpr("id = 0", Map("multiplication_result_x2" -> "-1"))
        deltaTable.updateExpr("id = 0", Map("multiplication_result_x2" -> "-1"))
      } finally {
        barrier.countDown()
      }
    }).start()
    new Thread(() => {
      try {
        println("Starting the 'update 100'")
        deltaTable.updateExpr("id = 0", Map("multiplication_result_x2" -> "100"))
        deltaTable.updateExpr("id = 0", Map("multiplication_result_x2" -> "100"))
        deltaTable.updateExpr("id = 0", Map("multiplication_result_x2" -> "100"))
      } finally {
        barrier.countDown()
      }
    }).start()
    barrier.await()
  }

}
