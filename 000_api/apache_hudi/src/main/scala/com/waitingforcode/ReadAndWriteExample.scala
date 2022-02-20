package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.hudi.DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.File


object ReadAndWriteExample {

  def main(args: Array[String]): Unit = {
    val outputDir = "/tmp/acid-file-formats/000_api/hudi"
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = SparkSession.builder()
      .appName("Read & write example").master("local[*]")
      .withExtensions(new HoodieSparkSessionExtension())
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    import sparkSession.implicits._
    val inputData = Seq(
      Order(1, 33.99d, "Order#1"), Order(2, 14.59d, "Order#2"), Order(3, 122d, "Order#3")
    ).toDF

    inputData.write.format("hudi").options(getQuickstartWriteConfigs)
      .option("hoodie.table.name", "orders")
      .option("hoodie.datasource.write.operation", INSERT_OPERATION_OPT_VAL)
      .option("hoodie.datasource.write.recordkey.field", "id")
      .mode(SaveMode.Overwrite)
      .save(outputDir)

    val allReadOrders = sparkSession.read.format("hudi").load(outputDir)
    allReadOrders.createOrReplaceTempView("orders_table")
    sparkSession.read.format("hudi").load(outputDir).where("amount > 40").show(false)
    sparkSession.sql("SELECT * FROM orders_table WHERE amount > 40").show(false)
    sparkSession.table("orders_table").where("amount > 40").show(false)
  }

}

case class Order(id: Long, amount: Double, title: String)