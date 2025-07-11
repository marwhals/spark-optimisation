package part4_rdd_joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Cogrouping
 * - Use cogrouping to speed up joins
 */

object CogroupingRDDs {

  val spark = SparkSession.builder()
    .appName("Cogrouping RDDs")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  val rootFolder = "src/main/resources/generated/examData"

  /**
  Scenario
    Take all the student attempts
    - if a student passed (at least one attempt > 9.0), send them an email "PASSED"
    - else send them an email with "FAILED"
   */

  def readIds() = sc.textFile(s"$rootFolder/examIds.txt")
    .map { line =>
      val tokens = line.split(" ")
      (tokens(0).toLong, tokens(1))
    }

  def readExamScores() = sc.textFile(s"$rootFolder/examScores.txt")
    .map { line =>
      val tokens = line.split(" ")
      (tokens(0).toLong, tokens(1).toDouble)
    }

  def readExamEmails() = sc.textFile(s"$rootFolder/examEmails.txt")
    .map { line =>
      val tokens = line.split(" ")
      (tokens(0).toLong, tokens(1))
    }

  // Straightforward but not optimal
  def plainJoin() = {
    val scores: RDD[(Long, Double)] = readExamScores().reduceByKey(Math.max)
    val candidates: RDD[(Long, String)] = readIds()
    val emails: RDD[(Long, String)] = readExamEmails()

    val results: RDD[(Long, (String, String))] = candidates
      .join(scores) // RDD[(Long, (String, Double))]
      .join(emails) // RDD[(Long, ((String, Double), String))]
      .mapValues {
        case ((_, maxAttempt), email) =>
          if (maxAttempt >= 9.0) (email, "PASSED")
          else (email, "FAILED")
      }

    results.count()
  }

  def coGroupedJoin() = {
    val scores: RDD[(Long, Double)] = readExamScores().reduceByKey(Math.max)
    val candidates: RDD[(Long, String)] = readIds()
    val emails: RDD[(Long, String)] = readExamEmails()

    val result: RDD[(Long, Option[(String, String)])] = candidates.cogroup(scores, emails) // co-partition the 3 RDDs: RDD[(Long, (Iterable[String], Iterable[Double], Iterable[String]))]
      .mapValues {
        case (nameIterable, maxAttemptIterable, emailIterable) =>
          val name = nameIterable.headOption
          val maxScore = maxAttemptIterable.headOption
          val email = emailIterable.headOption

          for {
            e <- email
            s <- maxScore
          } yield (e, if (s >= 9.0) "PASSED" else "FAILED")
      }

    result.count()
    result.count()
  }


  def main(args: Array[String]): Unit = {
    plainJoin()
    coGroupedJoin()
    Thread.sleep(1000000)
  }

  /**
   * Key points:
   * - Make sure all the RDDs share the same partitioner
   * - Particularly useful for multi-way joins
   * -- All RDDs are shuffled at most once
   * -- RDDs are never shuffled again if cogrouped RDD is reused
   * - Keeps the entire data - equivalent to a full outer join
   * -- For each key, an iterator of values is given
   * -- If there is no value for a "column", the respective iterator is empty
   */

}
