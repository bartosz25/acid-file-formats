package com.waitingforcode

object FileSystemLayoutApp2Demo {

  def main(args: Array[String]): Unit = {
    val sparkSession = getDeltaLakeSparkSession()
    import sparkSession.implicits._
    sparkSession.catalog.listDatabases().show()
    sparkSession.catalog.listTables().show()
    sparkSession.sql("""UPDATE default.letters SET lowerCase = "aa" WHERE id = "A"""".stripMargin)
    sparkSession.sql("""DELETE FROM default.letters WHERE id = "C"""".stripMargin)

    val inputData = Seq(
      Letter("F", "f")
    ).toDF
    inputData.writeTo("letters").append()

    sparkSession.sql("SELECT * FROM letters").show(false)
  }

}
