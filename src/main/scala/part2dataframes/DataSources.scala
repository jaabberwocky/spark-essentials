package part2dataframes

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

//noinspection DuplicatedCode
object DataSources extends App {

  // need a spark session to connect to spark
  val spark = SparkSession.builder()
    .appName("DataSources")
    .config("spark.master", "local") // set one attribute at a time
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  val carsDateTypeSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))


  /*
  * Reading a DF
  * - format
  * - schema or inferSchema = true
  * - zero or more options
  * */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode" , "permissive")
    .load("src/main/resources/data/cars.json")

  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  /*
  * Writing DFs
  * - format
  * - save mode = overwrite, append, ignore, errorIfExists
  * - zero or more options
  * */
  println("WRITING DATAFRAMES IN JSON!")

  // trying out column rename
  val carsDFColRenamed = carsDF.withColumnRenamed("Name","CarName")
  carsDFColRenamed.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .option("path", "src/main/resources/data/cars_dupe.json")
    .save()

  // json flags
  val carsDateDF = spark.read
    .format("json")
    .option("dateFormat", "yyyy-MM-dd") // only works with enforced schema; will become null if spark fails to parse
    .schema(carsDateTypeSchema)
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
    .load("src/main/resources/data/cars.json")

  carsDateDF.show()
  carsDateDF.printSchema()

  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  val stocksDF = spark.read
    .schema(stocksSchema)
    .format("csv")
    .option("dateFormat", "MMM dd yyyy")
    .option("header","true")
    .option("sep",",")

}
