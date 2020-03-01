package com.bigdata.spark

import org.apache.spark.ml.feature.{RFormula, StringIndexer}
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, split}
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.elasticsearch.spark.sql._


object LifeExpectancy extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("LifeExpectancy")
    .config("spark.es.nodes", "127.0.0.1")
    .config("spark.es.port", "9200")
    .getOrCreate()

  val sc = spark.sparkContext

  val newDF = spark
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "lifeexpectancy", "keyspace" -> "streamingdata"))
    .load()

  //newDF.show()

  val modDF = newDF.withColumn("_tmp", split(col("data"), ",")).select(
    col("_tmp").getItem(0).as("id"),
    col("_tmp").getItem(1).as("Country"),
    col("_tmp").getItem(2).as("Year"),
    col("_tmp").getItem(3).as("Status"),
    col("_tmp").getItem(4).as("LifeExpec"),
    col("_tmp").getItem(5).as("AdultMortality"),
    col("_tmp").getItem(6).as("InfantDeaths"),
    col("_tmp").getItem(7).as("Alcohol"),
    col("_tmp").getItem(8).as("ExpensePrice"),
    col("_tmp").getItem(9).as("HepatitisB"),
    col("_tmp").getItem(10).as("Measles"),
    col("_tmp").getItem(11).as("BMI"),
    col("_tmp").getItem(12).as("UnderFiveDeaths"),
    col("_tmp").getItem(13).as("Polio"),
    col("_tmp").getItem(14).as("TotalExpense"),
    col("_tmp").getItem(15).as("Diphtheria"),
    col("_tmp").getItem(16).as("HIV"),
    col("_tmp").getItem(17).as("GDP"),
    col("_tmp").getItem(18).as("Population"),
    col("_tmp").getItem(19).as("thinness1"),
    col("_tmp").getItem(20).as("thinness2"),
    col("_tmp").getItem(21).as("Income"),
    col("_tmp").getItem(22).as("Schooling")
  ).drop("_tmp")

  //modDF.show()

  val indexer = new StringIndexer()
    .setInputCol("Country")
    .setOutputCol("CountryIndex")
    .fit(modDF)
  val indexDF = indexer.transform(modDF)

  //indexDF.show(10)

  val featCols = Array("CountryIndex", "Year", "AdultMortality", "InfantDeaths", "ExpensePrice",
    "HepatitisB", "Measles", "BMI", "UnderFiveDeaths", "TotalExpense", "GDP", "Population")

  var filterDF = indexDF
  for (c <- indexDF.columns) {
    if (featCols contains c)
      if (c == "Population")
        filterDF = filterDF.filter(col(c).isNotNull).withColumn(c, col(c).cast(IntegerType))
      else
        filterDF = filterDF.filter(col(c).isNotNull).withColumn(c, col(c).cast(DoubleType))
  }
  val filDF = filterDF.na.fill(0)
  //filDF.show()

  val colName = Seq("id", "Country", "CountryIndex", "Year", "Status", "LifeExpec", "AdultMortality", "InfantDeaths", "ExpensePrice",
    "HepatitisB", "Measles", "BMI", "UnderFiveDeaths", "TotalExpense", "GDP", "Population");
  var dataDF = filDF.select(colName.map(name => col(name)): _*).withColumn("LifeExpec1", lit(0));

  // dataDF.show()

  val formula = new RFormula().setFormula(
    """LifeExpec1 ~ CountryIndex + Year + AdultMortality + InfantDeaths + ExpensePrice
                                           + HepatitisB + Measles + BMI + UnderFiveDeaths + TotalExpense + GDP + Population""")
    .setFeaturesCol("features")
    .setLabelCol("label")

  var finalData = formula.fit(dataDF).transform(dataDF)

  finalData = finalData.na.fill(0)
  finalData.show()

  val sameModel: LinearRegressionModel = LinearRegressionModel.load("/Users/mahesh/IdeaProjects/LifeExpectancyRate/model/LinearReg")

  val pred = sameModel.transform(finalData)

  val output = pred.drop("LifeExpec1").drop("features")
  //val output = pred.select("Country","CountryIndex","Year","LifeExpec","AdultMortality","InfantDeaths","ExpensePrice","HepatitisB",
  //                        "Measles","BMI","UnderFiveDeaths","TotalExpense","GDP","Population","label","prediction")

  output.show()

  output.saveToEs("lifeexpectancydata")

  println("Data was successfully added to Elastic Search")
}
