package com.waitingforcode

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions

import java.io.File
import java.util.concurrent.CountDownLatch

object App2ConflictUpdateDifferentFilesSamePartition {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(2)
    import sparkSession.implicits._
    val tableName = "app2"

    val inputData = (0 to 200000).map(nr => (nr, nr * 2, nr * 3))
      .toDF("id", "multiplication_result_x2", "multiplication_result_x3")
      .repartition(2)
    inputData.write.format("delta").saveAsTable(tableName)
    val deltaTable = DeltaTable.forName(tableName)

    val idsToReadFromDifferentFiles = sparkSession.read.table(tableName)
      .select($"id", functions.input_file_name().as("file_name"))
      .groupBy("file_name")
      .agg(Map("id" -> "first"))
      .selectExpr("`first(id)` AS id").as[Int].collect()

    val barrier = new CountDownLatch(2)
    new Thread(() => {
      try {
        val idToUpdate = idsToReadFromDifferentFiles(0)
        println(s"Starting the #1 'update ${idToUpdate}'")
        deltaTable.updateExpr(s"id = ${idToUpdate}", Map("multiplication_result_x2" -> "-1"))

        println(s"Starting the #2 'update ${idToUpdate}'")
        DeltaTable.forName(tableName).updateExpr(s"id = ${idToUpdate}", Map("multiplication_result_x2" -> "-1"))
        println(s"Starting the #3 'update ${idToUpdate}'")
        DeltaTable.forName(tableName).updateExpr(s"id = ${idToUpdate}", Map("multiplication_result_x2" -> "-1"))
      } finally {
        barrier.countDown()
      }
    }).start()
    new Thread(() => {
      try {
        val idToUpdate = idsToReadFromDifferentFiles(1)
        println(s"Starting the #1 'update ${idToUpdate}'")
        deltaTable.updateExpr(s"id = ${idToUpdate}", Map("multiplication_result_x2" -> "100"))
        println(s"Starting the #2 'update ${idToUpdate}'")
        deltaTable.updateExpr(s"id = ${idToUpdate}", Map("multiplication_result_x2" -> "100"))
        println(s"Starting the #3 'update ${idToUpdate}'")
        deltaTable.updateExpr(s"id = ${idToUpdate}", Map("multiplication_result_x2" -> "100"))
      } finally {
        barrier.countDown()
      }
    }).start()
    barrier.await()

    sys.exit(1)
  }

}
