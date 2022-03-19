package com.waitingforcode

import org.apache.hudi.DataSourceWriteOptions.{DELETE_OPERATION_OPT_VAL, INSERT_OPERATION_OPT_VAL}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.spark.sql.SaveMode


object FileSystemLayoutApp2Demo {

  def main(args: Array[String]): Unit = {
    val sparkSession = getHudiSparkSession()
    import sparkSession.implicits._
    val updateDataset = Seq(
      Letter("A", "aa")
    ).toDF

    updateDataset.write.format("hudi").options(getQuickstartWriteConfigs)
      .option("hoodie.table.name", "letters")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "creationTime")
      .mode(SaveMode.Append)
      .save(outputDir)

    val deleteDataset = Seq(
      Letter("C", "c")
    ).toDF

    deleteDataset.write.format("hudi").options(getQuickstartWriteConfigs)
      .option("hoodie.table.name", "letters")
      .option("hoodie.datasource.write.operation", DELETE_OPERATION_OPT_VAL)
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "creationTime")
      .mode(SaveMode.Append)
      .save(outputDir)

    val inputData = Seq(
      Letter("F", "f")
    ).toDF

    inputData.write.format("hudi").options(getQuickstartWriteConfigs)
      .option("hoodie.table.name", "letters")
      .option("hoodie.datasource.write.operation", INSERT_OPERATION_OPT_VAL)
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "creationTime")
      .mode(SaveMode.Append)
      .save(outputDir)

    sparkSession.read.format("hudi").load(outputDir).show(false)
  }

}
