package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

import scala.math.Ordering.Implicits.infixOrderingOps

object examples {
  def exec1(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("/home/formation/Documents/BigData/Spark/data/tours.json")
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

  def exec1bis(): Unit={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val demoCommunesDF = spark.read
      .json("/home/formation/Documents/BigData/Spark/data/demographie_par_commune.json")

//    demoCommunesDF.select(sum($"Population"))
//      .show()

//    demoCommunesDF.groupBy($"Departement")
//      .agg(sum($"Population") as "sum_pop" ,$"Departement")
//      .orderBy($"sum_pop".desc)
//      .show()

    val nomDepDF = spark.read
      .csv("/home/formation/Documents/BigData/Spark/data/departements.txt")
      .withColumnRenamed("_c0","nom")
      .withColumnRenamed("_c1", "Departement")
      .show()

    demoCommunesDF.groupBy($"Departement")
      .agg(sum($"Population") as "sum_pop" ,$"Departement")
      .orderBy($"sum_pop".desc)
      .show()

//    demoCommunesDF.join(nomDepDF,"Departement").show()
  }


  def exec3(): Unit={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("/home/formation/Documents/BigData/Spark/data/tours.json")

//    toursDF.printSchema()

//    Show the number of unique levels difficulty
    toursDF.groupBy($"tourDifficulty")
      .agg(count($"tourDifficulty"))
      .show()

//    min,max, avg of tour prices
    toursDF.agg(min($"tourPrice"),max($"tourPrice"),avg($"tourPrice"))
      .show()

//    min,max,avg of tour price + min, max, avg duration for each difficulty
    toursDF.groupBy($"tourDifficulty")
      .agg(min($"tourPrice"),max($"tourPrice"),avg($"tourPrice"), min($"tourLength"),max($"tourLength"),avg($"tourLength"))
      .show()

//    top 10 tour tags
    toursDF.select($"tourName",explode($"tourTags") as "tags")
      .groupBy($"tags")
      .count()
      .sort($"count".desc)
      .limit(10)
      .show()

//    Relationship between top 10 tags and difficulty
    toursDF.select($"tourName",explode($"tourTags") as "tags", $"tourDifficulty")
      .groupBy($"tags",$"tourDifficulty")
      .count()
      .sort($"count".desc)
      .limit(10)
      .show()

//    Adding price avg, min, max + sort by avg
      toursDF.select(explode($"tourTags") as "tags", $"tourDifficulty",$"tourPrice")
        .groupBy($"tags",$"tourDifficulty")
        .agg(min("tourPrice"), max("tourPrice"), avg("tourPrice"))
        .sort($"avg(tourPrice)".desc)
        .limit(10)
        .show()


  }
}
