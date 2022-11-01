package com.waitingforcode

import org.apache.commons.io.FileUtils

import java.io.File

object App1SimpleReader {

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

    println("== SELECT ==")
    sparkSession.sql("SELECT * FROM local.db.letters").show(false)

    println("== load() ==")
    sparkSession.read.format("iceberg").load("/tmp/acid-file-formats/003_reading/iceberg/db/letters")
      .show(false)
  }

}
