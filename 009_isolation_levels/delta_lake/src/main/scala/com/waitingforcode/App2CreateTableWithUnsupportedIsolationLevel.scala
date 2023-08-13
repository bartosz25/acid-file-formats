package com.waitingforcode

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils

import java.io.File

object App2CreateTableWithUnsupportedIsolationLevel {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(2)
    import sparkSession.implicits._
    val tableName = "app2"

    sparkSession.sql(
      s"""
         |CREATE TABLE ${tableName} (id bigint) USING delta
         | TBLPROPERTIES ('delta.isolationLevel' = 'WriteSerializable')
         |""".stripMargin)

    (0 to 100).toDF("id").writeTo(tableName).using("delta").createOrReplace()

    DeltaTable.forName(tableName).toDF.show(false)
  }


}
