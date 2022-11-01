package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.config.HoodieCompactionConfig
import org.apache.spark.sql.SaveMode

import java.io.File

object App1Writing {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))
    val sparkSession = getHudiSparkSession()
    import sparkSession.implicits._


    // TODO: test different write.opreation
    // TODO: test different table types
    inputData.write.format("hudi").options(getQuickstartWriteConfigs)
      .option("hoodie.table.name", "letters")
      .option("hoodie.datasource.write.operation", INSERT_OPERATION_OPT_VAL)
      // TODO: use COPY_ON_WRITE to see that the compactio is not supported o nthat type of table
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), "MERGE_ON_READ")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.precombine.field", "upperCase")
      .option("hoodie.datasource.write.partitionpath.field", "creationTime")
      .mode(SaveMode.Overwrite)
      // TODO: disable compaction in the option
      .option(HoodieCompactionConfig.INLINE_COMPACT_PROP, "false")
      .option(DataSourceWriteOptions.ASYNC_COMPACT_ENABLE_OPT_KEY, "false")
      // TOOD: check this https://github.com/apache/hudi/issues/2276 to underestand this props
      .option("hoodie.compact.inline.max.delta.commits", 1)
      .option("hoodie.compact.inline", false)
      .option("hoodie.compact.schedule.inline", true)
//        .option("compaction.schedule.enable", true)
      .save(outputDir)

    hudiSparkSession.read.format("hudi").load(outputDir).show(false)
  }

}