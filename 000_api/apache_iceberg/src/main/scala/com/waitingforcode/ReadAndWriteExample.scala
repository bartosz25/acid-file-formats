package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
import org.apache.spark.sql.SparkSession

import java.io.File

object ReadAndWriteExample {

  def main(args: Array[String]): Unit = {
    val outputDir = "/tmp/acid-file-formats/000_api/iceberg"
    FileUtils.deleteDirectory(new File(outputDir))
    FileUtils.deleteDirectory(new File("/tmp/acid-file-formats/000_api/catalog"))

    val sparkSession = SparkSession.builder()
      .appName("Read & write example").master("local[*]")
      .withExtensions(new IcebergSparkSessionExtensions())
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", "file:///tmp/acid-file-formats/000_api/catalog")
      .getOrCreate()
    import sparkSession.implicits._
    val inputData = Seq(
      Order(1, 33.99d, "Order#1"), Order(2, 14.59d, "Order#2"), Order(3, 122d, "Order#3")
    ).toDF

    inputData.writeTo("local.db.orders_from_write_to").using("iceberg").createOrReplace()
    sparkSession.sql("DROP TABLE IF EXISTS orders_from_sql")
    sparkSession.sql(
      s"""
         |CREATE OR REPLACE TABLE local.db.orders_from_sql (
         | id LONG,
         | amount DOUBLE,
         | title STRING
         |) USING iceberg
         |""".stripMargin)
    sparkSession.sql(
      """
        |INSERT INTO local.db.orders_from_sql (id, amount, title) VALUES
        |(1, 33.99, "Order#1"), (2, 14.59, "Order#2"), (1, 122, "Order#3")
        |""".stripMargin)

    sparkSession.table("local.db.orders_from_write_to").where("amount > 40").show(false)
    sparkSession.sql("SELECT * FROM local.db.orders_from_write_to WHERE amount > 40").show(false)
    sparkSession.sql("SELECT * FROM local.db.orders_from_sql WHERE amount > 40").show(false)
  }

}

case class Order(id: Long, amount: Double, title: String)