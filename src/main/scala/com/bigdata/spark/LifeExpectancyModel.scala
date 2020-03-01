package com.bigdata.spark


import org.apache.spark.ml.feature.{RFormula, StringIndexer}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType}

object LifeExpectancyModel extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("LifeExpectancy")
    .getOrCreate()

  val sc = spark.sparkContext

  /*
  val newDF = spark
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(Map( "table" -> "input1", "keyspace" -> "streamingdata" ))
    //.schema(StructType(schema))
    .load()

  newDF.show()
  print(newDF.count())
  */

  val rawDF = spark.read
    .format("csv")
    .option("header", "true")
    .load("/Users/mahesh/Desktop/PBDA/Project/Life Expectancy Data_Copy.csv")

  val indexer = new StringIndexer()
    .setInputCol("Country")
    .setOutputCol("CountryIndex")
    .fit(rawDF)
  val indexDF = indexer.transform(rawDF)

  //indexDF.show(10)

  val featCols = Array("CountryIndex", "Year", "LifeExpec", "AdultMortality", "InfantDeaths", "ExpensePrice",
    "HepatitisB", "Measles", "BMI", "UnderFiveDeaths", "TotalExpense", "GDP", "Population");
  val colName = Seq("Country", "CountryIndex", "Year", "LifeExpec", "AdultMortality", "InfantDeaths", "ExpensePrice",
    "HepatitisB", "Measles", "BMI", "UnderFiveDeaths", "TotalExpense", "GDP", "Population");
  var filterDF = indexDF
  for (c <- indexDF.columns) {
    if (featCols contains c)
      if (c == "Population")
        filterDF = filterDF.filter(col(c).isNotNull).withColumn(c, col(c).cast(IntegerType))
      else
        filterDF = filterDF.filter(col(c).isNotNull).withColumn(c, col(c).cast(DoubleType))
  }
  //filterDF.show()
  val dataDF = filterDF.select(colName.map(name => col(name)): _*);
  val formula = new RFormula().setFormula(
    """LifeExpec ~ CountryIndex+Year+AdultMortality+InfantDeaths+ExpensePrice
                                           +HepatitisB+Measles+BMI+UnderFiveDeaths+TotalExpense+GDP+Population""")
    .setFeaturesCol("features")
    .setLabelCol("label")

  //dataDF.show()
  val finalData = formula.fit(dataDF).transform(dataDF)
  val linearRegression = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
  finalData.show()
  //train.show()

  val linearRegressionModel = linearRegression.fit(finalData)
  val pred = linearRegressionModel.transform(finalData)
  val Array(train, test) = finalData.randomSplit(Array(0.7, 0.3), seed = 5043)
  val output = pred.select("Country", "CountryIndex", "Year", "AdultMortality", "InfantDeaths", "ExpensePrice", "HepatitisB",
    "Measles", "BMI", "UnderFiveDeaths", "TotalExpense", "GDP", "Population", "label", "prediction").show()
  val a = linearRegressionModel.evaluate(test);

  linearRegressionModel.write.overwrite().save("model/LinearReg")
  print("Linear Regression Model is trained")


  println(" RootMean Square Error " + a.rootMeanSquaredError)

}
