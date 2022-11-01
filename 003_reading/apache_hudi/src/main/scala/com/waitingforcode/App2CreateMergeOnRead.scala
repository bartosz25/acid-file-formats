package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.hudi.DataSourceWriteOptions.{DELETE_OPERATION_OPT_VAL, INSERT_OPERATION_OPT_VAL, UPSERT_OPERATION_OPT_VAL}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.common.model.HoodieTableType
import org.apache.spark.sql.SaveMode

import java.io.File

object App2CreateMergeOnRead {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))
    val tableName = "letters_mor"

    // Let's create the table first
    inputData.write.format("hudi").options(getQuickstartWriteConfigs)
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", INSERT_OPERATION_OPT_VAL)
      .option("hoodie.datasource.write.table.type", HoodieTableType.MERGE_ON_READ.name())
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.partitionpath.field", "category")
      .option("hoodie.datasource.write.precombine.field", "creationTime")
      .mode(SaveMode.Overwrite)
      .save(outputDir)
    println("== AFTER WRITE#1 ==")
    hudiSparkSession.read.format("hudi").load(outputDir).show(false)

    // Now, we need to do an upsert (1 will be replaced, 4 will be added)
    Thread.sleep(2000L)
    import hudiSparkSession.implicits._
    val upsertInputData = Seq(
      Letter(1, "AA", "aa", "important"),
      Letter(4, "D", "d", "urgent")
    ).toDF
    upsertInputData.write.format("hudi").options(getQuickstartWriteConfigs)
      .option("hoodie.table.name", tableName)
      .option("hoodie.datasource.write.operation", UPSERT_OPERATION_OPT_VAL)
      .option("hoodie.datasource.write.table.type", HoodieTableType.MERGE_ON_READ.name())
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.partitionpath.field", "category")
      .option("hoodie.datasource.write.precombine.field", "creationTime")
      .mode(SaveMode.Append)
      .save(outputDir)
    println("== AFTER WRITE#2 ==")
    hudiSparkSession.read.format("hudi").load(outputDir).show(false)

    // Finally, let's delete one record
    val recordsToDeleteWithoutPartition = Seq((1)).toDF("id")
    val recordsToDelete = Seq((1, "important")).toDF("id", "category")
    Seq(recordsToDeleteWithoutPartition, recordsToDelete).foreach(dataFrameWithRecordsToDelete => {
      dataFrameWithRecordsToDelete.write.format("hudi").options(getQuickstartWriteConfigs)
        .option("hoodie.table.name", tableName)
        .option("hoodie.datasource.write.operation", DELETE_OPERATION_OPT_VAL)
        .option("hoodie.datasource.write.table.type", HoodieTableType.MERGE_ON_READ.name())
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.partitionpath.field", "category")
        .option("hoodie.datasource.write.precombine.field", "creationTime")
        .mode(SaveMode.Append)
        .save(outputDir)


      // For the dataset with the single id, Hudi shouldn't delete the record
      println(s"== AFTER WRITE#3 for ${dataFrameWithRecordsToDelete.schema.fieldNames.mkString(", ")} ==")
      hudiSparkSession.read.format("hudi").option("hoodie.datasource.query.type", "read_optimized").load(outputDir).show(false)
      hudiSparkSession.read.format("hudi").option("hoodie.datasource.query.type", "snapshot").load(outputDir).show(false)
    })
  }

}
