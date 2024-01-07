package com.waitingforcode

import org.apache.spark.sql.functions

object App2BatchWriter2AddNewColumns {

  def main(args: Array[String]): Unit = {
    val sparkSession = getDeltaLakeSparkSession(1)
    import sparkSession.implicits._
    val numbersWithLetters = Seq(
      (1, "a"), (2, "b"), (3, "c"), (4, "d")
    ).toDF("number", "letter")
      .withColumn("upper_letter", functions.upper($"letter"))

    sparkSession.sql(s"ALTER TABLE ${NumbersWithLettersTable} ADD COLUMN upper_letter STRING DEFAULT NULL")
    numbersWithLetters.write.format("delta").insertInto(NumbersWithLettersTable)
  }

}
