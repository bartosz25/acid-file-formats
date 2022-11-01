package com.waitingforcode

import org.apache.commons.io.FileUtils

import java.io.File

object App7GenerateOrcDatasets {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getIcebergSparkSession()
    import sparkSession.implicits._
    val inputData = Seq(
      Letter(1, "A", "a", NestedLetter("key-a", "value-a")),
      Letter(2, "B", "b", NestedLetter("key-b", "value-b")),
      Letter(3, "C", "c", NestedLetter("key-c", "value-c"))
    ).toDF

    inputData.writeTo("local.db.letters").option("write-format", "orc").using("iceberg").createOrReplace()

    inputData.writeTo("local.db.letters").append()
  }

}
