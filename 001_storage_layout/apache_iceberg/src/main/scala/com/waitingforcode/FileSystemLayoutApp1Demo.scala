package com.waitingforcode


object FileSystemLayoutApp1Demo {

  def main(args: Array[String]): Unit = {
    val sparkSession = getIcebergSparkSession()
    import sparkSession.implicits._
    val inputData = Seq(
      Letter("A", "a"), Letter("B", "b"), Letter("C", "c"), Letter("D", "d"), Letter("E", "e")
    ).toDF

    inputData.writeTo("local.db.letters").using("iceberg").createOrReplace()

    sparkSession.sql("SELECT * FROM local.db.letters").show(false)
  }

}
