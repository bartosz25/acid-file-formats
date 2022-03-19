package com.waitingforcode


object FileSystemLayoutApp2Demo {

  def main(args: Array[String]): Unit = {
    val sparkSession = getIcebergSparkSession()
    import sparkSession.implicits._

    sparkSession.sql("""UPDATE local.db.letters SET lowerCase = "aa" WHERE id = "A"""".stripMargin)
    sparkSession.sql("""DELETE FROM local.db.letters WHERE id = "C"""".stripMargin)

    val inputData = Seq(
      Letter("F", "f")
    ).toDF
    inputData.writeTo("local.db.letters").append()

    sparkSession.sql("SELECT * FROM local.db.letters").show(false)
  }

}
