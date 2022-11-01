package com.waitingforcode

import org.apache.commons.io.FileUtils

import java.io.File

object App2StaticDynamicOverwrite {

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

    val partitioningConfigs = Seq("STATIC", "DYNAMIC")
    partitioningConfigs.foreach(partitionOverwriteMode => {
      println(s"============== TESTING ${partitionOverwriteMode} ==============")

      inputData.writeTo("local.db.letters").partitionedBy(inputData("upperCase"))
        .using("iceberg").createOrReplace()

      sparkSession.sql("SELECT * FROM local.db.letters").show(false)
      // The query should:
      // * overwrite all partitions for the static partitioning
      // * overwrite only B partition for the dynamic partitioning
      sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", partitionOverwriteMode)
      sparkSession.sql(
        """
          |INSERT OVERWRITE local.db.letters
          |SELECT id*2, upperCase, lowerCase, nestedLetter, NOW() AS creationTime
          |FROM local.db.letters
          |WHERE upperCase = 'B'
          |""".stripMargin)
      sparkSession.sql("SELECT * FROM local.db.letters").show(false)
    })

    // Let's see how to use the static overwrite and impact only the "B" partition
    println("============== TESTING STATIC WITH PARTITION CLAUSE ==============")
    inputData.writeTo("local.db.letters").partitionedBy(inputData("upperCase"))
      .using("iceberg").createOrReplace()
    sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "STATIC")
    sparkSession.sql(
      """
        |INSERT OVERWRITE local.db.letters PARTITION(upperCase = 'B')
        |SELECT id*2 AS id, lowerCase, nestedLetter, NOW() AS creationTime
        |FROM local.db.letters
        |WHERE upperCase = 'B'
        |""".stripMargin)
    sparkSession.sql("SELECT * FROM local.db.letters").show(false)

  }

}
