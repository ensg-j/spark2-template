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
}
