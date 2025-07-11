package part4_rdd_joins

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Optimising RDD Joins
 * - Techinques to optimise key-value RDD joins
 * - Free-for-all techniques
 * -- Spark doesn't optimise our jobs with DFs/SparkSQL
 * -- Its the users job to make them run well
 * -- Optimised versions often work better than SparkSQL
 */

object SimpleRDDJoins {

  val spark = SparkSession.builder()
    .appName("RDD joins")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  val rootFolder = "src/main/resources/generated/examData"

  /**
   * Scenario:
   * We are running a nation wide standard exam with 1,000,000 students
   * - every candidate can attempt the exam 5 times
   * - each attempt is scored 0-10
   * - final score is the max of all attempts
   * Goal: The number of candidates who passed the exam
   * - Passed: at least one attempt above 9.0
   * Data:
   * - Candidates: Candidate ID (long), candidate name (string)
   * - Exam scores: candidate ID (long), attempt score (double)
   *
   */

  // DataGenerator.generateExamData(rootFolder, 1000000, 5)

  def readIds() = sc.textFile(s"$rootFolder/examIds.txt")
    .map { line =>
      val tokens = line.split(" ")
      (tokens(0).toLong, tokens(1))
    }
    .partitionBy(new HashPartitioner(10))

  def readExamScores() = sc.textFile(s"$rootFolder/examScores.txt")
    .map { line =>
      val tokens = line.split(" ")
      (tokens(0).toLong, tokens(1).toDouble)
    }

  // goal: determine the number of students who passed the exam (= at least one attempt > 9.0)

  def plainJoin() = {
    val candidates: RDD[(Long, String)] = readIds()
    val scores: RDD[(Long, Double)] = readExamScores()

    // simple join
    val joined: RDD[(Long, (Double, String))] = scores.join(candidates) // (score attempt, candidate name)
    val finalScores = joined
      .reduceByKey((pair1, pair2) => if(pair1._1 > pair2._1) pair1 else pair2)
      .filter(_._2._1 > 9.0)

    finalScores.count
  }

  def preAggregate() = {
    val candidates = readIds()
    val scores = readExamScores()

    // do aggregation first - 10% perf increase
    val maxScores: RDD[(Long, Double)] = scores.reduceByKey(Math.max)
    val finalScores = maxScores.join(candidates).filter(_._2._1 > 9.0)

    finalScores.count
  }

  def preFiltering() = {
    val candidates = readIds()
    val scores = readExamScores()

    // do filtering first before the join
    val maxScores = scores.reduceByKey(Math.max).filter(_._2 > 9.0)
    val finalScores = maxScores.join(candidates)

    finalScores.count
  }

  // Do so that the join operation does not incur a shuffle
  def coPartitioning() = {
    val candidates = readIds()
    val scores = readExamScores()

    val partitionerForScores = candidates.partitioner match {
      case None => new HashPartitioner(candidates.getNumPartitions)
      case Some(partitioner) => partitioner
    }

    val repartitionedScores = scores.partitionBy(partitionerForScores)
    val joined: RDD[(Long, (Double, String))] = repartitionedScores.join(candidates)
    val finalScores = joined
      .reduceByKey((pair1, pair2) => if(pair1._1 > pair2._1) pair1 else pair2)
      .filter(_._2._1 > 9.0)

    finalScores.count
  }

  def combined() = {
    val candidates = readIds()
    val scores = readExamScores()

    val partitionerForScores = candidates.partitioner match {
      case None => new HashPartitioner(candidates.getNumPartitions)
      case Some(partitioner) => partitioner
    }

    val repartitionedScores = scores.partitionBy(partitionerForScores)

    // do filtering first before the join
    val maxScores = repartitionedScores.reduceByKey(Math.max).filter(_._2 > 9.0)
    val finalScores = maxScores.join(candidates)

    finalScores.count
  }

  def main(args: Array[String]): Unit = {
    plainJoin()
    preAggregate()
    preFiltering()
    coPartitioning()
    combined()

    Thread.sleep(1000000)
  }

  /**
   * Summary
   * - Spark doesn't optimise out operations the same as it did with SQL
   * --- No query plans
   * --- No column pruning
   * --- No pre-filtering
   *
   * - Developer must do it
   * --- Pre-filter the data before the join
   * --- Pre-aggregate (even partial) results before the join
   * --- Co-partition: assign partitioners to RDDs so we can avoid shuffles
   * --- Combine all the above
   *
   *
   * --> Optimising Spark can be a bit of an Art
   */


}
