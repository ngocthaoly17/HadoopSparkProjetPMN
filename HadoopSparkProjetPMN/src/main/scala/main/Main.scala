package main

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.Row
import scala.collection.JavaConversions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.SparkContext
import utils.{Utils, UserDefinedFunction}

object Main extends App {

  val Utils = new Utils()
  val spark = Utils.ConnSparkSession.sparkSession
  spark.conf.set("spark.sql.leftJoin.enabled", "true")
  spark.conf.set("spark.sql.crossJoin.enabled", "true")
  spark.conf.set("spark.sql.leftOuterJoin.enabled", "true")

  val countryClassification = Utils.ReadCSV("country_classification.csv", spark)
  val goodsClassification = Utils.ReadCSV("goods_classification.csv", spark)
  val outputCSVFull = Utils.ReadCSV("output_csv_full.csv", spark)
  val serviceClassification = Utils.ReadCSV("services_classification.csv", spark)

  countryClassification.show()
  countryClassification.printSchema()

  goodsClassification.show()
  goodsClassification.printSchema()

  outputCSVFull.show()
  outputCSVFull.printSchema()

  serviceClassification.show()
  serviceClassification.printSchema()

  import spark.sqlContext.implicits._

  val dfResults = new DataFrameResult()
  // modifier la date
  var new_outputCSVFull = dfResults.convertDate(outputCSVFull, "time_ref")
  // ajouter l'année
  new_outputCSVFull = dfResults.addYear(outputCSVFull, "time_ref_date")
  // ajouter code pays
  new_outputCSVFull = dfResults.matchingNameCountry(new_outputCSVFull, countryClassification, "country_code", "country_code")
  new_outputCSVFull.show()
  // filtrer les services et ajouter service label
  var service_outputCSVFull = dfResults.addDetailService(new_outputCSVFull, serviceClassification, "code", "code")
  service_outputCSVFull.show()
  // filtrer les goods et ajouter les détails
  var goods_outputCSVFull = dfResults.addDetailGood(new_outputCSVFull, goodsClassification)
  // classement des pays exportateurs (par goods et par services)
  var services_rankingExportCountries = dfResults.rankExportCountries(service_outputCSVFull)
  services_rankingExportCountries.show()
  var goods_rankingExportCountries = dfResults.rankExportCountries(goods_outputCSVFull)
  goods_rankingExportCountries.show()

  var groupByGoods = dfResults.groupByGoods(service_outputCSVFull)
  groupByGoods.show()
  var groupByServices = dfResults.groupByServices(goods_outputCSVFull)
  groupByServices.show()

  var list_of_services = dfResults.listOfServiceExportedFrance(service_outputCSVFull)
  list_of_services.show()
  var list_of_goods = dfResults.listOfGoodsImportedFrance(goods_outputCSVFull)
  list_of_goods.show()
  // classement des services les moins demandés
  var rankAscService = dfResults.rankingAscServices(service_outputCSVFull)
  rankAscService.show()
  // classement des goods les plus demandé
  var rankAscGoods = dfResults.rankingAscServices(service_outputCSVFull)
  rankAscGoods.show()

