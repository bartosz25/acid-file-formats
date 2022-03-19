package com.waitingforcode

object FileSystemLayoutApp3Demo {

  def main(args: Array[String]): Unit = {
    val sparkSession = getDeltaLakeSparkSession()

    import sparkSession.implicits._

    val inputData = Seq(
      Letter("G", "g")
    ).toDF

    inputData.writeTo("letters").using("delta").createOrReplace()

    sparkSession.sql("SELECT * FROM letters").show(false)
  }

}
