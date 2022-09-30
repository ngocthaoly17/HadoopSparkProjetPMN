package utils
import com.univocity.parsers.csv.Csv
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkContext

class Utils {

  object ConnSparkSession  {
    val sparkSession = SparkSession.builder()
  def CreateSparkSession() = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("projetfinal")
      .getOrCreate()
  }

  def ReadCSV(fileName:String, conn: SparkSession): DataFrame  = {
      val data = conn.read
        .options(Map("header" -> "true", "infer schema" -> "true", "delimiter" -> ","))
        .csv("HadoopSparkProjetPMN/src/main/resources/" + fileName)
      println(data)
      return data
  }

  def WriteCSV(df:DataFrame, fileName:String): Unit = {
    df.write.format("csv").save("HadoopSparkProjetPMN/src/resources/%s.csv".format(fileName))
  }

  def WriteParquet(df:DataFrame, fileName:String): Unit = {
    df.write.parquet("HadoopSparkProjetPMN/src/resources/%s.parquet".format(fileName))

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
