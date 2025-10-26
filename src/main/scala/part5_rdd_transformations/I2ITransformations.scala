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
   * Science project
   * each metric has identifier, value
   *
   * Return the smallest ("best") 10 metrics (identifiers + values)
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
        - they are NARROW TRANSFORMATIONS  -- can be processed in parallel and independently. No shuffles between nodes
        - Spark will "selectively" spill data to disk when partitions are too big for memory

        Warning: don't traverse more than once or convert to collections otherwise everything will be loaded into memory, and you will lose the benefit of being able to spill over to disk
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
   * Exercises
   */

  /*
    Better than the "dummy" approach
    - we are not sorting the entire RDD

    Bad (Worse than the optimal)
    - We are sorting the entire partition
    - We are forcing the iterator in memory and this can "OOM exception" your executors

   */
  def printTopMetricsEx1() = {
    val topMetrics = readMetrics()
      .mapPartitions(_.toList.sortBy(_._2).take(LIMIT).iterator)
      .repartition(1)
      .mapPartitions(_.toList.sortBy(_._2).take(LIMIT).iterator)
      .take(LIMIT)

    topMetrics.foreach(println)
  }

  /**
   * Better than ex1
   * - extracting the top 10 values per partition instead of sorting the entire partition
   *
   * Bad
   * - toList forces the entire loading of data in memory --> OOM Exceptions
   * - This versions requires us to iterate over the data set twice
   * --- If the list is immutable, time spent allocating objects (and Garbage Collecting)
   *
   */

  def printTopMetricsEx2() = {
    val topMetrics = readMetrics()
      .mapPartitions { records =>
        implicit val ordering: Ordering[(String, Double)] = Ordering.by[(String, Double), Double](_._2)
        val limitedCollection = new mutable.TreeSet[(String, Double)]()

        records.toList.foreach { record => // to list forces the entire loading of data in memory
          limitedCollection.add(record)
          if (limitedCollection.size > LIMIT) {
            limitedCollection.remove(limitedCollection.last)
          }
        }
        // I've traversed the iterator
        limitedCollection.iterator
      }
      .repartition(1)
      .mapPartitions { records =>

        implicit val ordering: Ordering[(String, Double)] = Ordering.by[(String, Double), Double](_._2)
        val limitedCollection = new mutable.TreeSet[(String, Double)]()

        records.toList.foreach { record =>
          limitedCollection.add(record)
          if (limitedCollection.size > LIMIT) {
            limitedCollection.remove(limitedCollection.last)
          }
        }
        // I've traversed the iterator
        limitedCollection.iterator
      }
      .take(LIMIT)

    topMetrics.foreach(println)
  }


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
   * -- Collecting the iterator in memory
   * -- Multiple passes over the data
   * - Key lessons
   * -- Travers the iterator ONCE
   * -- ****Don't use collection conversions***
   */

}
