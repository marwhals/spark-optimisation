package part3_df_joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Essential join concepts in Spark DF context
 */

object JoinRecap {

  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("Joins Recap")
    .getOrCreate()

  val sc = spark.sparkContext

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands")

  // inner joins -- how to specify join conditions
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner") //inner is default

  // outer joins
  // left outer = everything in inner join + all the rows in the LEFT table, with nulls in the rows not passing the condition in the RIGHT table
  guitaristsDF.join(bandsDF, joinCondition, "left_outer")
  // right outer = everything in inner join + all the rows in the RIGHT table, with nulls in the rows not passing the condition in the LEFT table
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")
  // outer join = everything in left_outer + right_outer
  guitaristsDF.join(bandsDF, joinCondition, "outer")

  // Spark concepts: semi joins = everything in the left DF for which THERE IS a row in the right DF satisfying the condition
  // essentially a filter
  guitaristsDF.join(bandsDF, joinCondition, "left_semi")

  // anti join = everything in the left DF for which THERE IS NOT a row in the right DF satisfying the condition
  // also a filter
  guitaristsDF.join(bandsDF, joinCondition, "left_anti")

  // cross join = everything in the left table with everything in the right table
  // dangerous (high complexity): NRows(crossjoin) = NRows(left) x NRows(right) i.e a product of the number of elements of both data frames
  // careful with outer joins with non-unique keys

  // RDD joins
  val colorsScores = Seq(
    ("blue", 1),
    ("red", 4),
    ("green", 5),
    ("yellow", 2),
    ("orange", 3),
    ("cyan", 0)
  )
  val colorsRDD: RDD[(String, Int)] = sc.parallelize(colorsScores)
  val text = "The sky is blue, but the orange pale sun turns from yellow to red"
  val words = text.split(" ").map(_.toLowerCase()).map((_, 1)) // standard technique for counting words with RDDs
  val wordsRDD = sc.parallelize(words).reduceByKey(_ + _) // counting word occurrence
  val scores: RDD[(String, (Int, Int))] = wordsRDD.join(colorsRDD) // implied join type is INNER


  def main(args: Array[String]): Unit = {

  }

}
