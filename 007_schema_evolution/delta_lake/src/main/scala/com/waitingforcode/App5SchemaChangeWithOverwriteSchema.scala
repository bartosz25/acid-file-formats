package com.waitingforcode

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.delta.DeltaLog

import java.io.File

/**
Other changes, which are not eligible for schema evolution, require that the schema and data are overwritten by
adding .option("overwriteSchema", "true"). For example, in the case where the column “Foo”
was originally an integer data type and the new schema would be a string data type, then all of the Parquet (data)
files would need to be re-written.  Those changes include:
 */
object App5SchemaChangeWithOverwriteSchema {

  def main(args: Array[String]): Unit = {
    FileUtils.deleteDirectory(new File(outputDir))

    val sparkSession = getDeltaLakeSparkSession(2)

    import sparkSession.implicits._
    val inputData = (0 to 2).map(nr => (nr, nr)).toDF("id", "id_as_int")
    inputData.write.mode(SaveMode.Overwrite).format("delta").save(outputDir)

    val inputDataWithDivisionResult = (4 to 6).map(nr => (nr, nr.toString)).toDF("id", "id_as_int")
    inputDataWithDivisionResult.write
      .mode(SaveMode.Overwrite) // Overwrite schema won't work without the overwrite mode
      .option("overwriteSchema", "true").format("delta").save(outputDir)

    sparkSession.read.format("delta").load(outputDir).show(false)
  }

}
