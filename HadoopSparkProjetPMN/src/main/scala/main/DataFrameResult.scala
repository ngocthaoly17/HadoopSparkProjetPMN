package main

import org.apache.spark.sql.functions.{col, to_date}
import org.joda.time.DateTime
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


class DataFrameResult {

  def convertDate(df:DataFrame, colName:String): DataFrame = {
    // de 202206 en 01/06/2022
    val new_df = df.select(col("*"), to_date(col(colName), "yyyyMM").as("time_ref_date"))
      .drop(col("time_ref"))
    return new_df
  }

  def addYear(df:DataFrame, colName:String): DataFrame = {
    val new_df = df.withColumn("year", year(col("time_ref_date")))
    return new_df
  }

  def matchingNameCountry(df1:DataFrame, df2:DataFrame, key1:String, key2:String): DataFrame = {
  // Ajouter la column nom_Pays qui associe le code pays a son nom
    var new_df = df1.join(df2, df1(key1) ===  df2(key2), "inner")
    return new_df
  }

  def addDetailService(df1:DataFrame, df2:DataFrame, key1:String, key2:String): DataFrame = {
    //Ajouter une column détails service (filtrage sur service)
    var new_df = df1.filter(df1("product_type") === "Services")
    new_df = new_df.join(df2, new_df(key1) ===  df2(key2), "inner")
    return new_df
  }

  def addDetailGood(df1:DataFrame, df2:DataFrame): DataFrame = {
    //Ajouter une column détails Good (filtrage sur goods)
    var new_df = df1.filter(df1("product_type") === "Goods")
    new_df = new_df.join(df2, new_df("code") === df2("NZHSC_Level_1_Code_HS2"), "left")
    new_df = new_df.join(df2, new_df("code") === df2("NZHSC_Level_2_Code_HS4"), "left")
    return new_df
  }

  def rankExportCountries(df:DataFrame): DataFrame = {
    // classement des pays exportateurs (par goods et par services)
    var new_df = df.filter(df("account") === "Exports")
    var groupbycountries = new_df.groupBy("country_label")
      .agg(count("*").alias("count_country"))
    val windowSpec = Window.partitionBy("country_label").orderBy("count_country")
    groupbycountries = groupbycountries.withColumn("rank",rank().over(windowSpec))
    return groupbycountries

  }

  def rankImportCountries(df:DataFrame): DataFrame = {
    //classement des pays importateurs (par goods et par services)
    var new_df = df.filter(df("account") === "Imports")
    var groupbycountries = new_df.groupBy("country_label")
      .agg(count("*").alias("count_country"))
    val windowSpec = Window.partitionBy("country_label").orderBy("count_country")
    groupbycountries = groupbycountries.withColumn("rank", rank().over(windowSpec))
    return groupbycountries
  }


  def groupByGoods(df:DataFrame): DataFrame = {
    //regroupement par good
    var new_df = df.filter(df("product_type") === "Goods")
    var groupbygoods = new_df.groupBy("code").count()
    return groupbygoods
  }

  def groupByServices(df:DataFrame): DataFrame = {
    //regroupement par service
    var new_df = df.filter(df("product_type") === "Services")
    var groupbyservices = new_df.groupBy("code").count()
    return groupbyservices
  }

  def listOfServiceExportedFrance(df:DataFrame): DataFrame = {
    //la liste des services exporté de la france
    var new_df = df.filter(df("product_type") === "Services" && df("country_code") === "FR" && df("account") === "Exports").select("service_label").distinct
    return new_df
  }

  def listOfGoodsImportedFrance(df:DataFrame): DataFrame = {
    //la listes des goods importés de la france
    var new_df = df.filter(df("product_type") === "Goods" && df("country_code") === "FR" && df("account") === "Imports").select("NZHSC_Level_2").distinct
    return new_df

  }

  def rankingAscServices(df:DataFrame): DataFrame = {
    //classement des services les moins demandés
    var new_df = df.filter(df("product_type") === "Services")
    var countByServices = new_df.groupBy("code")
      .agg(count("*").alias("count_services"))
    val windowSpec = Window.partitionBy("code").orderBy(asc("count_services"))
    val rankAscServices = new_df.withColumn("rank", rank().over(windowSpec))
    return rankAscServices
  }

  def rankingDescGoods(df:DataFrame): DataFrame = {
    //classement des goods les plus demandé
    var new_df = df.filter(df("product_type") === "Goods")
    var countByServices = new_df.groupBy("code")
      .agg(count("*").alias("count_goods"))
    val windowSpec = Window.partitionBy("code").orderBy(desc("count_goods"))
    val rankAscServices = new_df.withColumn("rank", rank().over(windowSpec))
    return rankAscServices

  }


  def addColumnStatus(df:DataFrame): DataFrame = {
    //Ajouter la column status_import_export
    var new_df = df.withColumn("status_import_export",
      when((df.filter(df("account") === "Imports").count()) > (df.filter(df("account") === "Exports").count()), "neg")
    .otherwise((df.filter(df("account") === "Imports").count()) < (df.filter(df("account") === "Exports").count(), "pos")))
    return new_df
  }

  def addColumnDifference(df: DataFrame): DataFrame = {
  
  }

  def sumGoods(): Unit = {
    // Ajouter la column Somme_good qui va calculer la somme des
    //goods par pays
  }

  def sumService(): Unit = {
    // Ajouter la column Somme_good qui va calculer la somme des
    //goods par pays
  }

  def PorcentageGoods(): Unit = {
    // Ajouter la column pourcentages_good qui va Calculer le
    //pourcentage de la column good par rapport à tous les good d'un
    //seul pays (regroupement par import et export)

  }

  def porcentageServices(): Unit = {
    // Ajouter la column pourcentages_good qui va Calculer le
    //pourcentage de la column Services par rapport à tous les Services d'un
    //seul pays (regroupement par import et export)

  }

  def groupGoodsByTypes(): Unit = {
    //regrouper les goods selon leur type (Code HS2)

  }

  def classifyExportersCountryPetrol(): Unit = {
    //classement des pays exportateur de pétrole

  }

  def classifyImportersCountryMeat(): Unit = {
    //classement des pays importateur de viandes

  }

  def classifyCountriesMostinITDemand(): Unit = {
    //classement des pays qui ont le plus de demandes sur les services
    //informatique

  }

  def description(): Unit = {
    //(bonus) ajouter une column description

  }

