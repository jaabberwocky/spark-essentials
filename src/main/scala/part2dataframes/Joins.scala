package part2dataframes

import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions._

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // inner joins
  // this is default
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF
    .join(
      bandsDF,
      joinCondition,
      "inner" // default
    )

  guitaristsBandsDF.show()

  // left outer join = everything in inner join + all the rows in the LEFT table
  // nulls for when data is missing in table
  guitaristsDF.join(bandsDF, joinCondition, "left_outer").show()

  // right outer = everything in inner + all the rows in RIGHT table
  // nulls for when data is missing in table
  guitaristsDF.join(bandsDF, joinCondition, "right_outer").show

  // outer join = everything in inner join + all the rows in BOTH tables
  // nulls as usual
  guitaristsDF.join(bandsDF, joinCondition, "outer").show

  // semi-joins
  // containing all the rows in LEFT table where there are rows satisfying
  // join condition in right table
  guitaristsDF.join(bandsDF, joinCondition, "left_semi").show()

  // anti-join
  // keep the rows in LEFT table which have NO rows satisfying
  // join condition in the right table
  guitaristsDF.join(bandsDF, joinCondition, "left_anti").show()


  // things to bear in mind
  // guitaristsBandsDF.select("id", "band").show() // this crashes as there are two "id"s

  // option 1 - rename the column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // option 2 - drop the dupe column
  guitaristsBandsDF.drop(bandsDF.col("id"))

  // option 3 - rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))

  // using complex types
  // you can do joins on arrays
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"),
    expr("array_contains(guitars, guitarId)")).show()

  /*
  * Exercises
  *
  * - show all employees and their max salary
  * - show all employees who were never managers
  * - find the job titles of the best paid 10 employees in the company
  * */
  // helper function to read tables
  def getDFFromDB(tableName: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
      .option("user", "docker")
      .option("password", "docker")
      .option("dbtable", tableName)
      .load()
  }
  // Ex1

  /*
  * SELECT emp_no, MAX(salary) FROM salaries GROUP BY emp_no;
  * */
  val salariesDF = getDFFromDB("salaries")
  val employeesDF = getDFFromDB("employees")
  val deptManagersDF = getDFFromDB("dept_manager")
  val titlesDF = getDFFromDB("titles")

  val maxSalariesPerEmpNoDF = salariesDF.selectExpr(
    "emp_no",
    "salary"
  ).groupBy("emp_no")
    .max("salary")

  val employeesSalariesDF = employeesDF.join(maxSalariesPerEmpNoDF, "emp_no")
  employeesSalariesDF.show()

  // Ex 2

  /*
  * SELECT * from employees as e left join dept_manager as d on e.emp_no = d.emp_no where d.emp_no is null;
  * */

  val joinCondition1 = employeesDF.col("emp_no") === deptManagersDF.col("emp_no")
  employeesDF.join(deptManagersDF, joinCondition1, "left_anti").show()

  // Ex 3

  /*
  // NOTE the sql query is to find the latest salaries of all employees (not related)
  * select e.emp_no, e.salary, t.max_date
  * from
  * (
  * select emp_no, max(to_date) as max_date from salaries group by emp_no
  * ) t
  * inner join salaries as e on t.emp_no = e.emp_no and e.to_date = t.max_date;
  * */

  /*
  * select t.title, e.emp_no, e.salary from salaries as e order by e.salary desc limit 10
  * inner join titles as t
  * on e.emp_no = t.emp_no
  * */
  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeesDF = employeesSalariesDF.orderBy(col("max(salary)").desc).limit(10)
  val bestPaidJobsDF = bestPaidEmployeesDF.join(mostRecentJobTitlesDF, "emp_no")

  bestPaidJobsDF.show()

}
