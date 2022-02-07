package com.validation

import org.apache.spark.sql.functions.{col, collect_list, concat_ws, monotonically_increasing_id, sum}
import org.apache.spark.sql.{DataFrame, _}

class ReconValidation extends Step {

  /**
   * Read two source and target files, whether in S3 , HDFS , local file system
   * For example, for HDFS, "hdfs://nn1home:8020/input/war-peace.parquet"
   * For S3 location, "s3n://myBucket/myFile1.csv"
   * @param fileFormat could be parquet,csv,json  etc
   * @param filePath where the file reside in any of the storage
   * @return  DataFrame
   */
  // Read the source and target file
  def readFile(fileFormat: String, filePath: String): DataFrame = {
    spark.read.option("header", "true")
      .option("inferSchema", "true")
      .format(fileFormat)
      .load(filePath)
  }

  /**
   * Will calculate the number of records in source and target
   * @param df sourceDataFrame which have to compare with targetDataFrame
   * @param column of the source/target to be compare
   * @return  DataFrame
   */
  def rowsCount(df: DataFrame, column: List[String]): DataFrame = {
    val newDF = df.groupBy().sum(column: _*)
    val colRegex = raw"^.+\((.*?)\)".r
    val newCols = newDF.columns.map(x => col(x).as(colRegex.replaceAllIn(x, m => m.group(1))))
    val resultDF = newDF.select(newCols: _*)
      .na.fill(0)
      .withColumn("Column_Name", monotonically_increasing_id())
    resultDF
  }
  /**
   * Will join source and target dataframe inorder to get the extra records in source and target
   * @param joinType type of the join(left_anti)
   * @param columns of the the source/target dataframe excluding the primary key
   * @param sourceDF sourceDF
   * @param targetDF targetDF
   * @param primaryKey PrimaryKey of the source & Target it could be more than 1
   * @return  DataFrame
   */
  def joinDF(joinType: String,columns: List[String],sourceDF: DataFrame,targetDF: DataFrame,primaryKey: List[String]): DataFrame ={
    val resultSet = columns.map( (i => sourceDF.join(targetDF, primaryKey:+i, joinType).agg(sum(i).as(i))
      .na.fill(0)
      .withColumn("Column_Name", monotonically_increasing_id())))
      .reduce((x, y) => x.join(y,"Column_Name"))
    resultSet
  }

  /**
   * Will method will transpose the dataframe
   * @param df
   * @param columns of the the source/target dataframe excluding the primary key
   * @param pivotCol sourceDF
   * @return  DataFrame
   */
  def TransposeDF(df: DataFrame, columns: Seq[String], pivotCol: String): DataFrame = {
    val columnsValue = columns.map(x => "'" + x + "', " + x)
    val stackCols = columnsValue.mkString(",")
    val df_1 = df.selectExpr(pivotCol, "stack(" + columns.size + "," + stackCols + ")")
      .select(pivotCol, "col0", "col1")
    val transposeDF = df_1.groupBy(col("col0")).pivot(pivotCol)
      .agg(concat_ws("", collect_list(col("col1"))))
      .withColumnRenamed("col0", pivotCol)
    transposeDF
  }

  // Write DataFrame data to CSV file
  // finalDF.write.format("csv").option("header", true).mode("overwrite").save("/tmp/reconRes")

}
