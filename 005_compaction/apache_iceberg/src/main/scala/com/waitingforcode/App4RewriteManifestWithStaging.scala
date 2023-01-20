package com.waitingforcode

import org.apache.commons.io.FileUtils
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.actions.SparkActions

import java.io.File
import java.nio.file.{FileSystems, Paths, StandardWatchEventKinds}

object App4RewriteManifestWithStaging {

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

    // I'm defining here a staging location where Iceberg will first write manifest files before
    // committing them to the main directory
    // I add the watchers to see the created and deleted files in this staging location
    val stagingPath = Paths.get("/tmp/manifests_staging")
    val watcher = FileSystems.getDefault().newWatchService()
    val key = stagingPath.register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE,
      StandardWatchEventKinds.ENTRY_MODIFY)
    new Thread(new Runnable {
      override def run(): Unit = {
        while (true) {
          key.pollEvents().forEach(event => {
            println(event.kind().name() + " ==> " + event.context())
          })
        }
      }
    }).start()

    val lettersTable = Spark3Util.loadIcebergTable(sparkSession, "local.db.letters")
    SparkActions
      .get()
      .rewriteManifests(lettersTable)
      .stagingLocation("/tmp/manifests_staging")
      .execute()


    println(">> metadata after the rewrite")
    sparkSession.sql("SELECT * from local.db.letters.metadata_log_entries").show(false)
    // It should show 1 compacted manifest
    sparkSession.sql("SELECT * from local.db.letters.manifests").show(false)
  }

}
