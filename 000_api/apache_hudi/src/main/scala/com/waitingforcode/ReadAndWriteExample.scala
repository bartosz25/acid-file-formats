package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.hudi.DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.File


object ReadAndWriteExample {

  def main(args: Array[String]): Unit = {
    val outputDir = "/tmp/acid-file-formats/000_api/hudi"
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = SparkSession.builder()
      .appName("Read & write example").master("local[*]")
      // TODO: why Kryo ?
      // TODO: why the dependnecies require spark-avro?
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    import sparkSession.implicits._
    val inputData = Seq(
      Order(1, 33.99d, "Order#1"), Order(2, 14.59d, "Order#2"), Order(3, 122d, "Order#3")
    ).toDF

    inputData.write.format("hudi").options(getQuickstartWriteConfigs)
      // TODO: Hudi has some constants for that, eg. here HoodieWriteConfig.TBL_NAME
      // TODO: class has Hoodie, project is called Hudi, why???
      .option("hoodie.table.name", "orders")
      .option("hoodie.datasource.write.operation", INSERT_OPERATION_OPT_VAL)
      .option("hoodie.datasource.write.partitionpath.field", "")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .mode(SaveMode.Overwrite)
      .save(outputDir)
    // First run: got this error: Exception in thread "main" org.apache.hudi.exception.HoodieException:
    //          hoodie only support org.apache.spark.serializer.KryoSerializer as spark.serializer
    // To solve it, I added  .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // TODO: why Kryo is supported?

    // Second run: this time another error: org.apache.hudi.exception.HoodieException: ts(Part -ts) field not found in record
    //        Added .option("hoodie.datasource.write.operation", INSERT_OPERATION_OPT_VAL) to solve this

    // Third run: recordKey value: "null" for field: "uuid" cannot be null or empty.
    //        Added .option("hoodie.datasource.write.partitionpath.field", "") to prevent partitioning
    //        (check https://hudi.apache.org/docs/writing_data/#spark-datasource-writer for PARTITIONPATH_FIELD_OPT_KEY)
    //    <!> but it didn't help: apparently, it's required to define a key for the written record (https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=113709185#FAQ-HowdoImodelthedatastoredinHudi)
    // added this ==> .option("hoodie.datasource.write.recordkey.field", "id") and the write worked!


    // The reading part was much easier since it relies on the Spark API
    // But the show contains some interesting details about the stored info:
    // _hoodie_commit_time|_hoodie_commit_seqno |_hoodie_record_key|_hoodie_partition_path|_hoodie_file_name                                                      |id |amount
    val allReadOrders = sparkSession.read.format("hudi").load(outputDir)
    allReadOrders.filter("amount > 40").show(false)

    // allReadOrders.createOrReplaceTempView("orders_table")
    // Alternative to the temporary view is an external table
    sparkSession.sql(s"CREATE TABLE orders_table USING hudi LOCATION '${outputDir}'")
    sparkSession.sql("SELECT * FROM orders_table WHERE amount > 40").show(false)
  }

}

case class Order(id: Long, amount: Double, title: String)