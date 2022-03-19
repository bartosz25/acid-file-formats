package com.waitingforcode

import org.apache.hudi.DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.spark.sql.SaveMode


object FileSystemLayoutApp3Demo {

  def main(args: Array[String]): Unit = {
    val sparkSession = getHudiSparkSession()
    import sparkSession.implicits._
    val inputData = Seq(
      Letter("G", "g")
    ).toDF

    inputData.write.format("hudi").options(getQuickstartWriteConfigs)
      .option("hoodie.table.name", "letters")
      .option("hoodie.datasource.write.operation", INSERT_OPERATION_OPT_VAL)
      .option("hoodie.datasource.write.recordkey.field", "id")
      .mode(SaveMode.Overwrite)
      .save(outputDir)

    sparkSession.read.format("hudi").load(outputDir).show(false)
  }

}
