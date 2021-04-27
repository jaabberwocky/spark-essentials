package part2dataframes

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._

object Aggregations extends App {

  val spark = SparkSession.builder()
      .appName("Aggregations and Grouping")
      .config("spark.master", "local")
      .getOrCreate()

  val moviesDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/movies.json")

  // count
  val genresCountDF = moviesDF.select(count(col("Major_Genre")).as("COUNT"))
  genresCountDF.show()
  moviesDF.selectExpr("count(Major_Genre)").show() // also equiv

  // count all
  moviesDF.select(count("*")).show() // show all, including NULLS

  // counting distinct
  moviesDF.select(countDistinct(col("Major_Genre"))).show()

  // approximate count
  moviesDF.select(approx_count_distinct(col("Major_Genre")))

  // min and max
  val minRatingDF = moviesDF.select(functions.min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")

  // sum
  moviesDF.select(sum(col("US_Gross")))
  moviesDF.selectExpr("sum(US_Gross)").show()

  // avg
  moviesDF.select(avg(col("US_Gross")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)").show

  // data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  ).show

  // Grouping
  val countByGenreDF = moviesDF.groupBy(col("Major_Genre"))
    .avg("Rotten_Tomatoes_rating")
    .as("avg_rating")
  countByGenreDF.show()

  // Grouping and Multiple Aggregations
  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))


  /*
  * Exercises
  *
  * 1. Sum up all the profits of all the movies in DF
  * 2. Count how many distinct directors we have in the DF
  * 3. Show the mean and stddev of US gross revenue for the movies
  * 4. Find out and compute the average IMDB rating and the average US gross revenue PER director
  *   - good to do a sort
  *
  * */

  // Ex1
  val sumOfAllProfitsDF = moviesDF.select(
    (
      sum("US_DVD_Sales")
      + sum("US_Gross")
      + sum("Worldwide_Gross")
      - sum("Production_Budget")
      )
      .as("Total_Profits")
  )
  sumOfAllProfitsDF.show

  // Ex2
  val distinctDirectors = moviesDF.select(countDistinct("Director"))
  distinctDirectors.show

  // Ex3
  val meanAndSD_DF = moviesDF
    .select(
      mean("US_Gross"),
      stddev("US_Gross")
    )
  meanAndSD_DF.show()

  // Ex4
  val averageRatingAndRevDF = moviesDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_IMDB_Rating"),
      avg("US_Gross").as("Avg_US_Gross"),
      count("*").as("Num_movies")
    )
    .orderBy(
      col("Avg_IMDB_Rating").desc,
      col("Avg_US_Gross").desc,
      col("Num_movies").desc
    )
  averageRatingAndRevDF.show()

}
