package com.waitingforcode

import org.apache.commons.io.FileUtils

import java.io.File

object App4RowWriteDisabled {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))
    val tableName = "letters_sql"
    hudiSparkSession.sql(
      s"""
         |CREATE TABLE $tableName (
         |  id INT,
         |  name STRING
         |) USING hudi
         | TBLPROPERTIES (primaryKey = 'id')
       """.stripMargin)
    hudiSparkSession.sql("set hoodie.sql.insert.mode = non-strict")
    hudiSparkSession.sql("set hoodie.sql.bulk.insert.enable = true")
    hudiSparkSession.sql("set hoodie.datasource.write.row.writer.enable = true")
    hudiSparkSession.sql("set hoodie.datasource.write.row.writer.enable = false")
    hudiSparkSession.sql(
      s"""
        |INSERT INTO ${tableName} VALUES
        |(1, 'a'), (2, 'b'), (3, 'c')
        |""".stripMargin)

    println("== AFTER WRITE#1 ==")
    hudiSparkSession.sql(s"SELECT * FROM ${tableName}").show(false)
  }

}
