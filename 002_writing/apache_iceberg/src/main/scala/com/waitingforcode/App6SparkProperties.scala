package com.waitingforcode

import org.apache.commons.io.FileUtils

import java.io.File

// Maybe I'm doing something wrong but I couldn't get it work
//
object App6SparkProperties {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getIcebergSparkSession()

    sparkSession.sql(
      """
        |CREATE OR REPLACE TABLE local.db.letters (
        |  id STRING,
        |  letter1 STRING,
        |  letter2 STRING
        |) USING iceberg
        |""".stripMargin)
    import sparkSession.implicits._
    val data = Seq(
      ("2", "b", "B")
    ).toDF("id", "letter1", "letter2")

    data.select("letter2", "letter1", "id").write
      .mode("append")
      .option("check-ordering", "false").insertInto("local.db.letters")

    //sparkSession.sql("SELECT '1' AS id, 'a' AS letter").writeTo("local.db.letters").append()
    //sparkSession.sql("SELECT 'a' AS letter, '2' AS id").printSchema()
    sparkSession.sql("SELECT '2' AS id, 'a' AS letter1, 'A' AS letter2")
      .select("letter2", "id" , "letter1").write
        .option("check-ordering", "true").insertInto("local.db.letters")
      //.option("check-ordering", true).append()
    //  .option("mergeSchema", "false")
    sparkSession.sql("SELECT * FROM local.db.letters").show(false)
  }

}
