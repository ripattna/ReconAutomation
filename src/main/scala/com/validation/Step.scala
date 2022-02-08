package com.validation

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

class Step {

  // Reading the conf file
  val config: Config = ConfigFactory.load("Config/application.conf")

  // Reading the Spark Environment
  val masterEnv: String = config.getString("sparkEnvironment.master")
  val appName: String = config.getString("sparkEnvironment.appName")

  // Spark Session
  val spark = SparkSession
    .builder()
    .master(masterEnv)
    .appName(appName)
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

}
