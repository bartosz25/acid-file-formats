package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.hudi.DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.common.model.HoodieTableType
import org.apache.spark.sql.SaveMode

import java.io.File

object App3SchemaReconciliation {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))
    val tableName = "letters_cow"

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Some problems encountered here because of:
    // * the creationTime - serialization issue
    // * reconciliation - values shift; id=5 has the lowerCase removed but it takes the value of the category column!
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    inputData.drop("creationTime").write.format("hudi").options(getQuickstartWriteConfigs)
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", INSERT_OPERATION_OPT_VAL)
      .option("hoodie.datasource.write.table.type", HoodieTableType.COPY_ON_WRITE.name())
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.partitionpath.field", "category")
      .mode(SaveMode.Overwrite)
      .save(outputDir)
    println("== AFTER WRITE#1 ==")
    hudiSparkSession.read.format("hudi").load(outputDir).show(false)

    import hudiSparkSession.implicits._
    val dataWithMissingColumns = Seq(
      Letter(5L, "e", "E", "important")
    ).toDF.drop("lowerCase")
    dataWithMissingColumns.drop("creationTime").write.format("hudi").options(getQuickstartWriteConfigs)
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", INSERT_OPERATION_OPT_VAL)
      .option("hoodie.datasource.write.table.type", HoodieTableType.COPY_ON_WRITE.name())
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.partitionpath.field", "category")
      .option("hoodie.datasource.write.reconcile.schema", true) // Enable/disable to see the writing work/fail
      .mode(SaveMode.Append)
      .save(outputDir)
    println("== AFTER WRITE#2 ==")
    hudiSparkSession.read.format("hudi").load(outputDir).show(false)
  }

}
