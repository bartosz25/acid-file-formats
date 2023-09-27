package com.waitingforcode

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils

import java.io.File

object App1VacuumFailedWithRetentionTooLow {
  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession()
    import sparkSession.implicits._
    val tableName = "app1"

    (0 to 100).toDF("id").writeTo(tableName).using("delta")
      .tableProperty("delta.deletedFileRetentionDuration", "interval 3 weeks")
      .createOrReplace()


    sparkSession.sql(s"DESCRIBE DETAIL $tableName")
      .show(truncate = false)

    // The VACUUM retention is lower than the table's retention; it should result in this error:
    // Exception in thread "main" java.lang.IllegalArgumentException: requirement failed: Are you sure you would like to vacuum files with such a low retention period? If you have
    //writers that are currently writing to this table, there is a risk that you may corrupt the
    //state of your Delta table.
    //
    //If you are certain that there are no operations being performed on this table, such as
    //insert/upsert/delete/optimize, then you may turn off this check by setting:
    //spark.databricks.delta.retentionDurationCheck.enabled = false
    //
    // If you are not sure, please use a value not less than "504 hours".
    DeltaTable.forName(tableName).vacuum(retentionHours = 2d)
  }



}
