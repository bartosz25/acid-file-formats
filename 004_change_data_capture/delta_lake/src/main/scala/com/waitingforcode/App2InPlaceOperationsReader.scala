package com.waitingforcode

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.streaming.Trigger

import java.io.File

object App2InPlaceOperationsReader {

  def main(args: Array[String]): Unit = {
    val sparkSession = getDeltaLakeSparkSession(1)
    FileUtils.deleteDirectory(new File(outputDir))
    val tableName = "rateDataInPlace"

    import sparkSession.implicits._
    Seq(User(1, "user1", true), User(2, "user2", false), User(3, "user3", true), User(4, "user4", true),
      User(5, "", true))
      .toDF
      .write.format("delta").saveAsTable(tableName)

    val deltaTable = DeltaTable.forName(tableName)

    // First update - delete a user with an empty login
    deltaTable.delete("login = ''")

    // Second update - update all active flags
    deltaTable.updateExpr("login = 'user4'", Map("isActive" -> "false"))

    // Third update - the merge
    val usersToUpdate = Seq(User(4, "user4", false), User(6, "user6", true)).toDF()
    deltaTable.as("users").merge(
      usersToUpdate.as("users_to_update"), "users.id = users_to_update.id"
    )
    .whenMatched().updateExpr(Map(
      "login" -> "users_to_update.login",
      "isActive" -> "users_to_update.isActive"
    ))
//    .whenMatched().delete()
    .whenNotMatched().insertExpr(Map(
      "id" -> "users_to_update.id",
      "login" -> "users_to_update.login",
      "isActive" -> "users_to_update.isActive"
    ))
    .execute()

    // These 2 events shouldn't generate any AddCDCFile action in the commit log
    deltaTable.delete()
    Seq(User(1, "user1", true), User(2, "user2", false)).toDF.write.format("delta").insertInto(tableName)

    // The code uses a streaming broker but you can also have a batch reader here defined as:
    // sparkSession.read.option("readChangeFeed", true).table(tableName).show(false)
    val cdcQuery = sparkSession.readStream.format("delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", 0).table(tableName)
      .writeStream.format("console").trigger(Trigger.Once).start()

    sparkSession.streams.awaitAnyTermination()
  }

}
