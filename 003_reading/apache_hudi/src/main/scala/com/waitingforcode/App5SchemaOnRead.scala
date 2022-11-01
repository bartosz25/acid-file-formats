package com.waitingforcode

import org.apache.commons.io.FileUtils

import java.io.File

object App5SchemaOnRead {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))
    val tableName = "letters_cow"
    hudiSparkSession.sql("set hoodie.schema.on.read.enable = true")
    hudiSparkSession.sql(
      s"""
         |CREATE TABLE $tableName (
         |  id INT,
         |  name STRING
         |) USING hudi
         | TBLPROPERTIES (primaryKey = 'id')
       """.stripMargin)
    hudiSparkSession.sql(
      s"""
         |INSERT INTO ${tableName} VALUES
         |(1, 'a'), (2, 'b'), (3, 'c')
         |""".stripMargin)
    hudiSparkSession.sql(s"ALTER TABLE ${tableName} ADD COLUMNS(extra_col STRING)")
   // val schema = hudiSparkSession.read.format("hudi").load(outputDir).schema
    //    schema.printTreeString()
    hudiSparkSession.sql(s"SELECT * FROM ${tableName}").show(false)

    hudiSparkSession.read
      .option("hoodie.schema.on.read.enable", true) // it's only to test thee BaseFileOnlyRelation
      .schema("id LONG, upperCase STRING, lowerCase STRING, category STRING").format("hudi")
      .load(outputDir).show(false)
  }

}
