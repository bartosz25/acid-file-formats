package com.waitingforcode


object FileSystemLayoutApp3Demo {

  def main(args: Array[String]): Unit = {
    val sparkSession = getIcebergSparkSession()

    import sparkSession.implicits._

    val inputData = Seq(
      Letter("G", "g")
    ).toDF

    inputData.writeTo("local.db.letters").using("iceberg").createOrReplace()

    sparkSession.sql("SELECT * FROM local.db.letters").show(false)
  }

}
