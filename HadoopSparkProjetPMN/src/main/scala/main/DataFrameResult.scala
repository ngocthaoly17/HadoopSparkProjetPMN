package main

import org.joda.time.DateTime

class DataFrameResult {


  def convertDate(dateValue:String): Unit = {
    // de 202206 en 01/06/2022
  }

  def addYear(date:DateTime): Unit = {

  }

  def matchingNameCountry(): Unit = {
  // Ajouter la column nom_Pays qui associe le code pays a son nom
  }

  def addDetailService(): Unit = {
    //Ajouter une column détails service (filtrage sur service)

  }

  def addDetailGood(): Unit = {
    //Ajouter une column détails Good (filtrage sur goods)

  }

  def classifyExportCountries(): Unit = {
    //classement des pays exportateurs (par goods et par services)

  }

  def classifyImportCountries(): Unit = {
    //classement des pays importateurs (par goods et par services)

  }

  def groupByGoods(): Unit = {
    //regroupement par good

  }

  def groupByService(): Unit = {
    //regroupement par service

  }

  def listOfServiceExportedFrance(): Unit = {
    //la liste des services exporté de la france

  }

  def listOfGoodsImportedFrance(): Unit = {
    //la listes des goods importés de la france

  }

  def rankingAscServices(): Unit = {
    //classement des services les moins demandés

  }

  def rankingDescGoods(): Unit = {
    //classement des goods les plus demandé

  }

  def addColumnStatus(): Unit = {
    //Ajouter la column status_import_export

  }

  def addColumnDifference(): Unit = {
    // Ajouter la column difference_import_export : qui va calculer les
    //exports - imports (par pays)

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




}
