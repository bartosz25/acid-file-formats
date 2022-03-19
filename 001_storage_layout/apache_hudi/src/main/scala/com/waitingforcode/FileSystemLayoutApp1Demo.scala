package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.hudi.DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.spark.sql.SaveMode

import java.io.File


object FileSystemLayoutApp1Demo {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getHudiSparkSession()
    import sparkSession.implicits._
    val inputData = Seq(
      Letter("A", "a"), Letter("B", "b"), Letter("C", "c"), Letter("D", "d"), Letter("E", "e")
    ).toDF

    inputData.write.format("hudi").options(getQuickstartWriteConfigs)
      .option("hoodie.table.name", "letters")
      .option("hoodie.datasource.write.operation", INSERT_OPERATION_OPT_VAL)
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "creationTime")
      .mode(SaveMode.Overwrite)
      .save(outputDir)

    sparkSession.read.format("hudi").load(outputDir).show(false)
  }

}
