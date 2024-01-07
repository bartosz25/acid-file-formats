package com.waitingforcode

object App3BatchWriter3RenameStreamedColumnWithSchemaOverwrite {

  def main(args: Array[String]): Unit = {
    val sparkSession = getDeltaLakeSparkSession(1)
    import sparkSession.implicits._
    val numbersWithLetters = Seq(
      (1, "a"), (2, "b"), (3, "c"), (4, "d")
    ).toDF("id_number", "letter")

    sparkSession.sql(s"ALTER TABLE ${NumbersWithLettersTable} RENAME COLUMN number TO id_number")
    //sparkSession.sql(s"ALTER TABLE ${NumbersWithLettersTable} DROP COLUMN number")
    numbersWithLetters.write.format("delta").insertInto(NumbersWithLettersTable)
  }

}
