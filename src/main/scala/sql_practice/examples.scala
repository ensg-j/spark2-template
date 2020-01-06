package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object examples {
  def exec1(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    println(toursDF
      .select(explode($"tourTags"))
      .groupBy("col")
      .count()
      .count()
    )

    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)

    toursDF.select($"tourPrice")
      .filter($"tourPrice" > 500)
      .orderBy($"tourPrice".desc)
      .show(20)


  }

  def exec2(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val hab = spark.read
      .json("data/input/demographie_par_commune.json")

    hab.agg(sum($"Population").as("Total_population"))
      .show()

    val top_populated_departement = hab.groupBy("Departement")
      .agg(sum($"Population").as("Sum_pop"))
      .orderBy($"Sum_pop".desc)

    top_populated_departement.show()

    val departement = spark.read.csv("data/input/departements.txt")
      .select($"_c0".as("departementName"),$"_c1".as("code"))

    val jointure = top_populated_departement.join(departement,$"Departement"===$"code")

    jointure.select($"Sum_pop",$"departementName")
      .show()
  }

  def exec2_sample(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._
    var sample_07 = spark.read.format("csv")
      .option("delimiter", "\t")
      .load("data/input/sample_07")
        .select($"_c0".as("id"),
          $"_c1".as("occupation"),
          $"_c2".as("number_07"),
          $"_c3".as("salary_07"))

    val sample_08 = spark.read.format("csv")
      .option("delimiter", "\t")
      .load("data/input/sample_08")
      .select($"_c0".as("id"),
        $"_c1".as("occupation"),
        $"_c2".as("number_08"),
        $"_c3".as("salary_08"))

    val top_salaries = sample_07.where($"salary_07">100000)
      .orderBy($"salary_07".desc)
    top_salaries.select($"occupation",$"salary_07").show()

    val salary_growth = sample_07.join(sample_08,Seq("occupation"))
      .select($"occupation",($"salary_08"-$"salary_07").as("salary_growth"),$"salary_07",$"salary_08")
      .where($"salary_growth">0)
      .orderBy($"salary_growth".desc)
    salary_growth.show()

    val jobs_loss = sample_07.join(sample_08,Seq("occupation"))
      .where($"salary_07">100000)
      .select($"occupation",($"number_08"-$"number_07").as("jobs_loss"),$"number_07",$"number_08")
      .where($"jobs_loss"<0)
      .orderBy($"jobs_loss")

    jobs_loss.show()

  }

  def exec3(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show()

    val toursDifficulties = toursDF.select($"tourDifficulty")
      .distinct()
    toursDifficulties.show()

    val min_max_avg_price = toursDF.select(avg($"tourPrice").as("Average_Price"),
      min($"tourPrice").as("Minimum_Price"),
      max($"tourPrice").as("Maximum_Price"))
    min_max_avg_price.show()

    val avg_by_difficulty = toursDF.groupBy($"tourDifficulty")
      .agg(min($"tourPrice").as("Minimum_Price"),
        avg($"tourPrice").as("Average_Price"),
        max($"tourPrice").as("Maximum_Price"),
        min($"tourLength").as("Minimum_Duration"),
        avg($"tourLength").as("Average_Duration"),
        max($"tourLength").as("Maximum_Duration"))
    avg_by_difficulty.show()

    val tourTags = toursDF.select(explode($"tourTags"))
      .groupBy($"col")
      .count()
      .orderBy($"count".desc)
    tourTags.show(10)

    val relationTagsDifficulty = toursDF.select(explode($"tourTags"),$"tourDifficulty")
      .groupBy($"col",$"tourDifficulty")
      .count()
      .orderBy($"count".desc)
    relationTagsDifficulty.show(10)

    val avg_relationTagsDiffic = toursDF.select(explode($"tourTags"),$"tourDifficulty",$"tourPrice")
      .groupBy($"col",$"tourDifficulty")
      .agg(min($"tourPrice").as("Minimum_Price"),
        avg($"tourPrice").as("Average_Price"),
        max($"tourPrice").as("Maximum_Price"))
      .orderBy($"Average_Price")
    avg_relationTagsDiffic.show()
  }
}
