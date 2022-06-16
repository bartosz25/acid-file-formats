package com.waitingforcode

import org.apache.commons.io.FileUtils

import java.io.File

object App1GenerateDatasetsWithConstraints {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession()

    println("=========== NULL CHECK CONSTRAINT =============")
    val constraintsTable = "letters_constraints"
    sparkSession.sql(
      s"""
         |CREATE OR REPLACE TABLE ${constraintsTable} (
         | id INT NOT NULL,
         | upperCase STRING NOT NULL
         |) USING DELTA
         |""".stripMargin)
    try {
      sparkSession.sql(
        s"""
           |INSERT INTO ${constraintsTable} VALUES (1, 'A'), (2, NULL), (NULL, 'C')
           |""".stripMargin)
    } catch {
      case ive: Exception => {
        ive.printStackTrace()
      }
    }

    sparkSession.sql(
      s"""
         |INSERT INTO ${constraintsTable} VALUES (4, 'D'), (5, 'E'), (6, 'F')
         |""".stripMargin)
    sparkSession.sql(s"SELECT * FROM ${constraintsTable}").show(false)

    // Let's add an extra constraint on the value
    println("=========== LENGTH CONSTRAINT =============")
    sparkSession.sql(s"ALTER TABLE ${constraintsTable} ADD CONSTRAINT upperCaseLength CHECK (LENGTH(upperCase) = 1)")
    try {
      sparkSession.sql(s"INSERT INTO ${constraintsTable} VALUES (7, 'GG')")
    }
    catch {
      case ive: Exception => {
        ive.printStackTrace()
      }
    }
    sparkSession.sql(s"INSERT INTO ${constraintsTable} VALUES (7, 'G')")
    sparkSession.sql(s"SELECT * FROM ${constraintsTable}").show(false)
  }

}
