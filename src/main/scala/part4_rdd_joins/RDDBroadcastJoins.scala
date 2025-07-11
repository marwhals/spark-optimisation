package part4_rdd_joins

import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
 * - RDD Broadcasting technique
 * - Recap
 * --- Used for joining a large "table" with a small "table". Example: a lookup table
 * --- Copy the small "table" entirely on to all the executors
 * ---> Leads to no shuffling
 * ---> Leads to much faster performance
 * Bigger the data the better the performance enhancement
 */

object RDDBroadcastJoins {

  val spark = SparkSession.builder()
    .appName("Broadcast Joins")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  val random = new Random()

  /*
    Scenario: assign prizes to a wide-scale competition (10M+ people).
    Goal: find out who won what.
   */

  // small lookup table
  val prizes = sc.parallelize(List(
    (1, "gold"),
    (2, "silver"),
    (3, "bronze")
  ))

  // the competition has ended - the leaderboard is known
  val leaderboard = sc.parallelize(1 to 10000000).map((_, random.alphanumeric.take(8).mkString))
  val medalists = leaderboard.join(prizes)
  medalists.foreach(println) // 38s for 10M elements!

  /**
    We know from SQL joins that the small RDD can be broadcast so that we can avoid the shuffle on the big RDD.
    However, for the RDD API, this has to be done manually.
  */

  //Step 1 - need to collect the RDD locally, so that we can broadcast to the executors
  val medalsMap = prizes.collectAsMap()
  //Step 2 -  after we do this, all executors can refer to the medalsMap locally
  sc.broadcast(medalsMap)
  //Step 3 -  need to avoid shuffles by manually going through the partitions of the big RDD
  // This lambda is being exucuted on every single executor
  val improvedMedalists = leaderboard.mapPartitions { iterator => // iterator of all the tuples in this partition; all the tuples are local to this executor
    iterator.flatMap { record =>
      val (index, name) = record
      medalsMap.get(index) match { // notice you can refer to the name medalsMap, which you now have access to locally after the broadcast
        case None => Seq.empty
        case Some(medal) => Seq((name, medal))
      }
    }
  }

  improvedMedalists.foreach(println) // 2s, blazing fast, no shuffles or anything at all.

  def main(args: Array[String]): Unit = {
    Thread.sleep(1000000)
  }

  /**
   * Key points:
   * - Broadcasting is useful when one RDD is small
   * -- Send it to all executors
   * -- No shuffles are needed
   * - Need to do broadcasting ourselves
   * -- Collect the small RDD locally
   * -- Call broadcast on the SparkContext
   * -- mapPartitions on the big RDD
   * -- use the collection locally in executors
   */

}
