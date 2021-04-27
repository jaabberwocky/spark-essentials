package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, expr}

object ColumnsAndExpression extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  carsDF.show()

  // Columns
  val firstColumn = carsDF.col("Name")

  // selecting
  // note you got a new dataframe from this
  // (projection)
  val carNames = carsDF.select(firstColumn)

  // various select methods
  carsDF.select(
    carsDF.col("Name"),
    carsDF.col("Acceleration")
  )

  // can be tedious, so we use another version

  import spark.implicits._

  carsDF.select(
    col("Name"),
    col("Acceleration"),
    'Year,
    expr("Origin")
  )

  // or can just use simple strings
  carsDF.select("Name", "Year")

  // EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  // then use the expression in a SELECT operation
  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg2") // use a SQL statement
  )

  // selectExpr
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2 as JAMESBOND"
  )

  carsWithWeightsDF.show()
  carsWithSelectExprWeightsDF.show()

  // DF processing using withColumn
  // adding a column
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)

  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")

  // escape spaces with backticks
  carsWithColumnRenamed.selectExpr("`Weight in pounds` + 100 as `This is very good`").show()

  // filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")

  // filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  americanCarsDF.show()

  // chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDF3 = carsDF
    .filter("Origin = 'USA' and Horsepower > 150")
    .orderBy(col("Horsepower").desc) // sort in descending order
  americanPowerfulCarsDF3.show()

  // union-ing = adding more rows
  val moreCarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF)
  allCarsDF.show()

  // distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show()

  /*
  * Exercises
  *
  * 1. Read the movies json file and select 2 columns of your choice.
  * 2. Create a new DF by summing up all the gross profits of the movie (US GROSS + US DVD SALES + WORLDWIDE GROSS)
  * 3. Select all movies which are "comedy" in genre with IMDB rating above 6
  *
  * Use as many versions/ways as possible.
  *
  * */

  // Ex1
  val moviesDF = spark.read
    .format("json")
    .option("inferSchema","true")
    .option("path", "src/main/resources/data/movies.json")
    .load()

  val moviesDFTwoCols1 = moviesDF.select(
    col("Title"),
    col("Production_Budget")
  )

  val moviesDFTwoCols2 = moviesDF.selectExpr("Title", "Worldwide_Gross")

  val moviesDFTwoCols3 = moviesDF.select(
    moviesDF.col("Title"),
    moviesDF.col("Worldwide_Gross")
  )

  moviesDFTwoCols1.show()
  moviesDFTwoCols2.show()
  moviesDFTwoCols3.show()

  // Ex2
  val grossProfitsDF1 = moviesDF.selectExpr(
    "Title",
    "US_Gross + Worldwide_Gross + coalesce(US_DVD_Sales,0) as Gross_Profits" // need coalesce since presence of nulls
  )

  grossProfitsDF1.show()

  // Ex3
  val comedyIMDB6_DF1 = moviesDF.selectExpr(
    "Title",
    "Major_Genre",
    "IMDB_Rating"
  ).filter(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)
    .orderBy(col("IMDB_Rating").desc)

  comedyIMDB6_DF1.show()

}
