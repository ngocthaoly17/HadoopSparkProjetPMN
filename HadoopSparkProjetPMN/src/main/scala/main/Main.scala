package main

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.Row
import scala.collection.JavaConversions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.SparkContext
import utils._


class Main {

  val spark = Utils.CreateSparkSession()

  val countryClassification = ReadCSV("country_classification.csv")
  val goodsClassification = ReadCSV("goods_classification.csv")
  val outputCSVFull = ReadCSV("output_csv_full.csv")
  val serviceClassification = ReadCSV("services_classification.csv")

}