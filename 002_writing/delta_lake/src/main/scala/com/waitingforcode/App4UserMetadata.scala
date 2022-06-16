package com.waitingforcode

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode

import java.io.File

object App4UserMetadata {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession()
    import sparkSession.implicits._
    val inputData = Seq(
      Letter(1, "A", "a", NestedLetter("key-a", "value-a")),
      Letter(2, "B", "b", NestedLetter("key-b", "value-b")),
      Letter(3, "C", "c", NestedLetter("key-c", "value-c"))
    ).toDF

    // The userMetadata can also be set aside
    /*
    sparkSession.sql(
      """
        |SET spark.databricks.delta.commitInfo.userMetadata={
        |    "author":"waitingforcode.com",
        |    "context":"Blog post about writing in Delta Lake"
        |};
        |""".stripMargin)
    */
    inputData.write.format("delta")
      // Even though we put here a struct, it's stored as a string in Delta Lake
      .option("userMetadata",
        """
          |{"author":"waitingforcode.com", "context":"Blog post about writing in Delta Lake"}
          |""".stripMargin)
      .mode(SaveMode.Overwrite).save(outputDir)

    val deltaTable = DeltaTable.forPath(sparkSession, outputDir)
    deltaTable.history
      .select("version", "operation", "userMetadata")
      .show(false)
    // To show the string type of the userMetadata
    deltaTable.history.printSchema()
  }

}
