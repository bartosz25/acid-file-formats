package com.waitingforcode

import io.delta.sql.DeltaSparkSessionExtension
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import java.io.File

object ReadAndWriteExample {

  def main(args: Array[String]): Unit = {
    val outputDir = "/tmp/acid-file-formats/000_api/delta_lake"
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = SparkSession.builder()
      .appName("API example").master("local[*]")
      .withExtensions(new DeltaSparkSessionExtension())
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    import sparkSession.implicits._
    val inputData = Seq(
      Order(1, 33.99d, "Order#1"), Order(2, 14.59d, "Order#2"), Order(3, 122d, "Order#3")
    ).toDF
    inputData.write.format("delta").save(outputDir)
    sparkSession.sql(s"CREATE TABLE default.orders USING DELTA LOCATION '${outputDir}'")
    inputData.writeTo("orders_from_write_to").using("delta").createOrReplace()
    sparkSession.sql("DROP TABLE IF EXISTS orders_from_sql")
    sparkSession.sql(
      s"""
        |CREATE OR REPLACE TABLE orders_from_sql (
        | id LONG,
        | amount DOUBLE,
        | title STRING
        |) USING delta LOCATION "${outputDir}/orders_from_sql_${System.currentTimeMillis()}"
        |""".stripMargin)
    sparkSession.sql(
      """
        |INSERT INTO orders_from_sql (id, amount, title) VALUES
        |(1, 33.99, "Order#1"), (2, 14.59, "Order#2"), (1, 122, "Order#3")
        |""".stripMargin)

    sparkSession.table("orders_from_write_to").where("amount > 40").show(false)
    sparkSession.read.format("delta").load(outputDir).where("amount > 40").show(false)
    sparkSession.sql("SELECT * FROM default.orders WHERE amount > 40").show(false)
    sparkSession.sql("SELECT * FROM orders_from_write_to WHERE amount > 40").show(false)
    sparkSession.sql("SELECT * FROM orders_from_sql WHERE amount > 40").show(false)
  }

}

case class Order(id: Long, amount: Double, title: String)