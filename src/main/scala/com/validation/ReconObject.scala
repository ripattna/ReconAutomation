package com.validation

import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object ReconObject{

  def main(args: Array[String]): Unit = {

    // Reading the conf file
    val config: Config = ConfigFactory.load("Config/application.conf")

    // Reading the Spark Environment
    val masterEnv: String = config.getString("sparkEnvironment.master")
    val appName: String = config.getString("sparkEnvironment.appName")

    // Reading the source and target file from config
    val sourcePath: String = config.getString("filePath.sourceFile")
    val targetPath: String = config.getString("filePath.targetFile")

    // Reading the file format from config
    val fileFormat: String = config.getString("fileFormat.fileFormat")

    // Reading the PrimaryKey from config
    val primaryKeyList = config.getStringList("primaryKey.primaryKeyValue").toList

    val sourceDF = new ReconValidation().readFile(fileFormat, sourcePath)
    sourceDF.show()
    val targetDF = new ReconValidation().readFile(fileFormat, targetPath)
    targetDF.show()

    // Schema of Source Data in List
    val schemaSchemaList = sourceDF.columns.toList
    // println(schemaSchemaList)

    // Columns to select after ignoring Primary Key
    val columnToSelect = schemaSchemaList diff primaryKeyList
    // println(columnToSelect)

    val sourceRowCount = new ReconValidation().rowsCount(sourceDF, columnToSelect)
    // sourceRowCount.show()
    val targetRowCount = new ReconValidation().rowsCount(targetDF, columnToSelect)
    // targetRowCount.show()

    // Overlap Records
    val overlapRowCount = new ReconValidation()
      .joinDF("inner", columnToSelect, sourceDF, targetDF, primaryKeyList)
    // overlapRowCount.show()

    // Extra Records in Source
    val extraSourceRowCount = new ReconValidation()
      .joinDF("left_anti", columnToSelect, sourceDF, targetDF, primaryKeyList)
    // extraSourceRowCount.show()

    // Extra Records in Target
    val extraTargetRowCount = new ReconValidation()
      .joinDF("left_anti",  columnToSelect, targetDF, sourceDF, primaryKeyList)
    // extraTargetRowCount.show()

    // Transpose the result
    val sourceRowsCount = new ReconValidation()
      .TransposeDF(sourceRowCount, columnToSelect, "Column_Name")
      .withColumnRenamed("0","Source_Rec_Count")

    val targetRowsCount = new ReconValidation()
      .TransposeDF(targetRowCount, columnToSelect, "Column_Name")
      .withColumnRenamed("0","Target_Rec_Count")

    val overlapRowsCount = new ReconValidation()
      .TransposeDF(overlapRowCount, columnToSelect, "Column_Name")
      .withColumnRenamed("0","Overlap_Rec_Count")

    val extraSourceRowsCount = new ReconValidation()
      .TransposeDF(extraSourceRowCount, columnToSelect, "Column_Name")
      .withColumnRenamed("0","Source_Extra_Rec_Count")

    val extraTargetRowsCount = new ReconValidation()
      .TransposeDF(extraTargetRowCount, columnToSelect, "Column_Name")
      .withColumnRenamed("0","Target_Extra_Rec_Count")

    // Final Result DF
    val finalDF = sourceRowsCount
      .join(targetRowsCount, Seq("Column_Name"),"inner")
      .join(overlapRowsCount, Seq("Column_Name"),"inner")
      .join(extraSourceRowsCount, Seq("Column_Name"),"inner")
      .join(extraTargetRowsCount, Seq("Column_Name"),"inner")
    finalDF.show()

    // Write DataFrame data to CSV file
    // finalDF.write.format("csv").option("header", true).mode("overwrite").save("/tmp/reconRes")

  }
}
