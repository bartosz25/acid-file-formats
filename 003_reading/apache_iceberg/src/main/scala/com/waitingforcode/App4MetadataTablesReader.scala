package com.waitingforcode

import org.apache.commons.io.FileUtils

import java.io.File

object App4MetadataTablesReader {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getIcebergSparkSession()
    import sparkSession.implicits._
    val inputData = Seq(
      Letter(1, "A", "a", NestedLetter("key-a", "value-a")),
      Letter(11, "A", "aa", NestedLetter("key-a", "value-a")),
      Letter(2, "B", "b", NestedLetter("key-b", "value-b")),
      Letter(22, "B", "bb", NestedLetter("key-b", "value-b")),
      Letter(3, "C", "c", NestedLetter("key-c", "value-c")),
      Letter(33, "C", "cc", NestedLetter("key-c", "value-c")),
      Letter(333, "C", "ccc", NestedLetter("key-c", "value-c"))
    ).toDF


    inputData.writeTo("local.db.letters").using("iceberg").createOrReplace()

    sparkSession.sql("SELECT * FROM local.db.letters.history").show(false)
  }

}
