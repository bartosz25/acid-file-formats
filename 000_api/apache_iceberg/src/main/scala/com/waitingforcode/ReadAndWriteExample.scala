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
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hive")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", "file:///tmp/acid-file-formats/000_api/catalog")
      .getOrCreate()
    import sparkSession.implicits._
    val inputData = Seq(
      Order(1, 33.99d, "Order#1"), Order(2, 14.59d, "Order#2"), Order(3, 122d, "Order#3")
    ).toDF
    // #1. It works! Spark3 runtime really means Spark 3.0 runtime!!!!
    //     No more class incompatibility errors!

    // Different semantic for writes https://iceberg.apache.org/#spark-writes/ (create, overwrite, etc.)
    // #2. Error: The namespace in session catalog must have exactly one name part: prod.db.table;
    //    ==> Changed to local.db.orders <=== TODO: What does each of the parts mean?
    //    ==> Probably the format comes from Apache Spark 3.0 (catalog.database.table)
    //        (check the blue box on https://iceberg.apache.org/#spark-queries/#inspecting-tables)
    //        and in the config you can find I'm using "spark.sql.catalog.local"
    inputData.writeTo("local.db.orders").create()

    sparkSession.table("local.db.orders").where("amount > 40").show(false)

    // #3. Here we don't need to create a temporary view or anything else; the table is already in the catalog
    //     so we can get it!
    sparkSession.sql("SELECT * FROM local.db.orders WHERE amount > 40").show(false)
  }

}

case class Order(id: Long, amount: Double, title: String)