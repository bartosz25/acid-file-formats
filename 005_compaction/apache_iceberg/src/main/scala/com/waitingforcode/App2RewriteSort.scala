package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.iceberg.SortOrder
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.actions.SparkActions

import java.io.File

object App2RewriteSort {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getIcebergSparkSession()
    import sparkSession.implicits._
    val inputData = (0 to 10000).map(nr => Letter(nr, nr.toChar.toString.toUpperCase, nr.toChar.toString)).toDF
      .repartition(400)


    inputData.writeTo("local.db.letters").using("iceberg").createOrReplace()
    println(">> files before the rewrite")
    sparkSession.sql("SELECT * from local.db.letters.files").show(false)

    val lettersTable = Spark3Util.loadIcebergTable(sparkSession, "local.db.letters")
    SparkActions
      .get()
      .rewriteDataFiles(lettersTable)
      .sort(SortOrder.builderFor(lettersTable.schema()).asc("id").build())
      .option("target-file-size-bytes", (500000).toString) // 500KB
      .execute()


    println(">> files after the rewrite")
    sparkSession.sql("SELECT * from local.db.letters.files").show(false)

    println(">> other metadata")
    sparkSession.sql("SELECT * from local.db.letters.metadata_log_entries").show(false)
    sparkSession.sql("SELECT * from local.db.letters.manifests").show(false)
  }

}
