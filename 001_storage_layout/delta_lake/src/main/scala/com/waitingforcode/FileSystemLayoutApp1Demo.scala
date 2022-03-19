package com.waitingforcode

import org.apache.spark.sql.SaveMode

object FileSystemLayoutApp1Demo {

  def main(args: Array[String]): Unit = {
    val sparkSession = getDeltaLakeSparkSession()
    import sparkSession.implicits._
    val inputData = Seq(
      Letter("A", "a"), Letter("B", "b"), Letter("C", "c"), Letter("D", "d"), Letter("E", "e")
    ).toDF
    inputData.write.mode(SaveMode.Overwrite).format("delta").save(outputDir)
    sparkSession.sql(s"CREATE TABLE letters USING DELTA LOCATION '${outputDir}'")

    sparkSession.sql("SELECT * FROM letters").show(false)
    sparkSession.catalog.listDatabases().show()
    sparkSession.catalog.listTables().show()
  }

}
