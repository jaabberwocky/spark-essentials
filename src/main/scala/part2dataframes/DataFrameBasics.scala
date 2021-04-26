package part2dataframes

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}


object DataFrameBasics extends App {

  // need a spark session to connect to spark
  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local") // set one attribute at a time
    .getOrCreate()

  // reading a dataframe
  val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  // showing a dataframe
  firstDF.show(15)
  firstDF.printSchema()

  // take first 10 rows from a distributed
  // set of data
  // note: you get rows from the dataset (array of rows)
  firstDF.take(10).foreach(println)

  // spark types
  val longType = LongType

  //noinspection DuplicatedCode
  // schema
  // structtype with structfield
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

  // get schema from existing dataframe
  val carsDFSchema = firstDF.schema
  println(carsDFSchema)

  // how to read a df with your schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSchema)
    .load("src/main/resources/data/cars.json")

  carsDFWithSchema.show()
  carsDFWithSchema.printSchema()

  // create rows by hand
  // use Row factory method
  val myRow = Row("chevrolet chevelle malibu",18,8,307,130,3504,12.0,"1970-01-01","USA")

  // create a dataframe manually from a tuple of rows
  val cars = Seq(
    ("chevrolet chevelle malibu",18,8,307,130,3504,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15,8,350,165,3693,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18,8,318,150,3436,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16,8,304,150,3433,12.0,"1970-01-01","USA"),
    ("ford torino",17,8,302,140,3449,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15,8,429,198,4341,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14,8,454,220,4354,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14,8,440,215,4312,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14,8,455,225,4425,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15,8,390,190,3850,8.5,"1970-01-01","USA")
  )

  val manualCarsDF = spark.createDataFrame(cars) // schema auto-inferred
  manualCarsDF.show()


  // note: DFs have schemas, rows do not

  // create DFs with implicits
  // note: importing from spark object and the implicits from that
  import spark.implicits._
  val manualCarsDFWithImplicits = cars.toDF("Name", "MPG", "Cylinders",
    "Displacement", "HP", "Weight", "Acceleration", "Year", "Country")

  // inspect the schemas
  manualCarsDF.printSchema() // no colnames
  manualCarsDFWithImplicits.printSchema()


  /*
  * Exercises:
  * 1) Create a manual DF describing smartphones
  *   - make
  *   - model
  *   - screen dimensions
  *   - camera megapixels
  *
  * 2) Read another file from data/ folder
  *   - print its schema
  *   - count the num of rows (using count method)
  *
  * */

  // Ex1
  val phones = Seq(
    ("samsung", "s8", "2560x1440", 10.4),
    ("apple", "iphone", "1920x1080", 9.2),
    ("huawei", "mate pro", "2560x1440", 14.2)
  )

  val phonesDF = phones.toDF("make","model","screen_dim","camera_mp")
  phonesDF.printSchema()
  phonesDF.show()

  // Ex2
  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  moviesDF.show()
  moviesDF.printSchema()
  println(s"Row count: ${moviesDF.count()}")

}
