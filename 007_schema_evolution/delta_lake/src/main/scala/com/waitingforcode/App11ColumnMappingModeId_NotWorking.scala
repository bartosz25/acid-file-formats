package com.waitingforcode

import org.apache.commons.io.FileUtils

import java.io.File

/**
 * Note: I tried to create a test for the Icerberg-to-Delta conversion as here:
 * https://github.com/delta-io/delta/blob/master/delta-iceberg/integration_tests/iceberg_converter.py
 *
 * Unfortunately, it doesn't work because of this error:
 * ```
 * Caused by: java.lang.ClassNotFoundException: org.apache.spark.sql.delta.IcebergTable
 * at java.net.URLClassLoader.findClass(URLClassLoader.java:387)
 * at java.lang.ClassLoader.loadClass(ClassLoader.java:418)
 * ...
 * ```
 *
 * Any clue is welcome 🙏
 */
object App11ColumnMappingModeId_NotWorking {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(2)

    import sparkSession.implicits._
    val inputData = Seq(
      (1, "A", "a"),
      (2, "B", "b"),
      (3, "C", "c")
    ).toDF("id", "upper", "lower")
    inputData.writeTo("local.db.letters").using("iceberg").createOrReplace()


    sparkSession.sql(s"CONVERT TO DELTA iceberg.`$outputDir/warehouse-local/db/letters`")
    sparkSession.read.format("delta").load(s"$outputDir/warehouse-local/db/letters").show()
  }

}
