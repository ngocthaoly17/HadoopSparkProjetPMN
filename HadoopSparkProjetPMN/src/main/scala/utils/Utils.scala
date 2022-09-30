package utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkContext

class Utils {

  def CreateSparkSession() = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("projetfinal")
      .getOrCreate()
  }

  def ReadCSV(fileName:String): Unit = {
    val spark = CreateSparkSession()
    val data = spark.read
      .options(Map("header" -> "true", "inferschema" -> "true", "delimiter" -> ";"))
      .csv("src/resources/" + fileName)
    return data
  }

  def WriteCSV(fileName:String): Unit = {
    fileName.write.format("csv").save("src/resources/%s.csv".format(fileName))
  }

  def WriteParquet(fileName:String): Unit = {
    fileName.write.parquet("%s.parquet".format(fileName))
  }

}

