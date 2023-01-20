package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.actions.SparkActions

import java.io.File

object App3RewriteManifestWithCondition {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getIcebergSparkSession()
    import sparkSession.implicits._
    val inputData = (0 to 10000).map(nr => Letter(nr, nr.toChar.toString.toUpperCase, nr.toChar.toString)).toDF
      .repartition(400)


    inputData.writeTo("local.db.letters").using("iceberg").createOrReplace()
    (0 to 5).foreach(_ => inputData.writeTo("local.db.letters").append())
    println(">> metadata before the rewrite")
    sparkSession.sql("SELECT * from local.db.letters.metadata_log_entries").show(false)
    // We should see 7 manifests here
    sparkSession.sql("SELECT * from local.db.letters.manifests").show(false)

    val lettersTable = Spark3Util.loadIcebergTable(sparkSession, "local.db.letters")
    SparkActions
      .get()
      .rewriteManifests(lettersTable)
      .rewriteIf(manifestFile => manifestFile.addedFilesCount() < 100000)
      .execute()


    println(">> metadata after the rewrite")
    sparkSession.sql("SELECT * from local.db.letters.metadata_log_entries").show(false)
    // It should show 1 compacted manifest
    sparkSession.sql("SELECT * from local.db.letters.manifests").show(false)
  }

}
