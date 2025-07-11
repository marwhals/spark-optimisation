package part5_rdd_transformations

import data_generators.DataGenerator
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 * Iterator-to-iterator transformations
 * -- Manipulate RDD partitions in an arbitrary fashion
 */

object I2ITransformations {

  val spark = SparkSession.builder()
    .appName("I2I Transformations")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  /**
   * Scenario
    Science project
    each metric has identifier, value

    Return the smallest ("best") 10 metrics (identifiers + values)
   */

  val LIMIT = 10

  def readMetrics() = sc.textFile("src/main/resources/generated/metrics/metrics10m.txt")
    .map { line =>
      val tokens = line.split(" ")
      val name = tokens(0)
      val value = tokens(1)

      (name, value.toDouble)
    }

  def printTopMetrics() = {
    val sortedMetrics = readMetrics().sortBy(_._2).take(LIMIT)
    sortedMetrics.foreach(println)
  }

  def printTopMetricsI2I() = {

    val iteratorToIteratorTransformation = (records: Iterator[(String, Double)]) => {
      /*
        This is called an iterator-to-iterator transformation
        - they are NARROW TRANSFORMATIONS  -- can be processed in paralell and independnetly. No shuffles between nodes
        - Spark will "selectively" spill data to disk when partitions are too big for memory

        Warning: don't traverse more than once or convert to collections otherwise everything will be loaded into memory and you will lost the benefit of being able to spill over to disk
        */

      implicit val ordering: Ordering[(String, Double)] = Ordering.by[(String, Double), Double](_._2)
      val limitedCollection = new mutable.TreeSet[(String, Double)]()

      records.foreach { record =>
        limitedCollection.add(record)
        if (limitedCollection.size > LIMIT) {
          limitedCollection.remove(limitedCollection.last)
        }
      }

      // I've traversed the iterator

      limitedCollection.iterator
    }

    val topMetrics = readMetrics()
      .mapPartitions(iteratorToIteratorTransformation)
      .repartition(1)
      .mapPartitions(iteratorToIteratorTransformation)

    val result = topMetrics.take(LIMIT)
    result.foreach(println)
  }

  /**
   * TODO - Exercises
   */



  def main(args: Array[String]): Unit = {
    DataGenerator.generateMetrics("", 1000000)
    printTopMetrics()
    printTopMetricsI2I()
    Thread.sleep(1000000)
  }

  /**
   * Key points:
   * - I2I transformation: manipulate each partition with an iterator function
   * - Benefits
   * -- Any transformation per-partition is a narrow transformation
   * -- if partitions are too large, Spark will spill to disk what it can't fit in memory
   * - Anti-Patterns
   * -- Collecting the iterator in memeory
   * -- Multiple passes over the data
   * - Key lessons
   * -- Travers the iterator ONCE
   * -- ****Don't use collection conversions***
   */

}
