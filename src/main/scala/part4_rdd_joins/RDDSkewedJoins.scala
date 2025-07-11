package part4_rdd_joins

import data_generators.{DataGenerator, Laptop, LaptopOffer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Fix the data skew problem at the RDD level
 * Recap:
 * -- Disproportionate distribution of data
 * -- Straggling tasks
 */

object RDDSkewedJoins {
  val spark = SparkSession.builder()
    .appName("RDD Skewed Joins")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  /**
  Scenario
    An online store selling gaming laptops.
    2 laptops are "similar" if they have the same make & model, but proc speed within 0.1

    For each laptop configuration, we are interested in the average sale price of "similar" models.

    Acer Predator 2.9Ghz aylfaskjhrw -> average sale price of all Acer Predators with CPU speed between 2.8 and 3.0 GHz
   */

  val laptops: RDD[Laptop] = sc.parallelize(Seq.fill(40000)(DataGenerator.randomLaptop()))
  val laptopOffers: RDD[LaptopOffer] = sc.parallelize(Seq.fill(100000)(DataGenerator.randomLaptopOffer()))

  def plainJoin() = {
    val preparedLaptops: RDD[((String, String), (String, Double))] = laptops.map {
      case Laptop(registration, make, model, procSpeed) => ((make, model), (registration, procSpeed))
    }

    val preparedOffers: RDD[((String, String), (Double, Double))] = laptopOffers.map {
      case LaptopOffer(make, model, procSpeed, salePrice) => ((make, model), (procSpeed, salePrice))
    }

    val result = preparedLaptops.join(preparedOffers) // RDD[(make, model), ((reg, cpu), (cpu, salePrice)))]
      .filter {
        case ((make, model), ((reg, laptopCpu), (offerCpu, salePrice))) => Math.abs(laptopCpu - offerCpu) <= 0.1
      }
      .map {
        case ((make, model), ((reg, laptopCpu), (offerCpu, salePrice))) => (reg, salePrice)
      }
      .aggregateByKey((0.0, 0))(
        {
          case ((totalPrice, numPrices), salePrice) => (totalPrice + salePrice, numPrices + 1) // combine state with record
        },
        {
          case ((totalPrices1, numPrices1), (totalPrices2, numPrices2)) => (totalPrices1 + totalPrices2, numPrices1 + numPrices2) // combine 2 states into one
        }
      ) // RDD[(String, (Double, Int))]
      .mapValues {
        case (totalPrices, numPrices) => totalPrices / numPrices
      }

    result.count()
  }

  def noSkewJoin() = {
    val preparedLaptops = laptops
      .flatMap { laptop => //Duplicating data like with data frames
        Seq(
          laptop,
          laptop.copy(procSpeed = laptop.procSpeed - 0.1),
          laptop.copy(procSpeed = laptop.procSpeed + 0.1),
        )
      }
      .map {
        case Laptop(registration, make, model, procSpeed) => ((make, model, procSpeed), registration)
      }

    val preparedOffers = laptopOffers.map {
      case LaptopOffer(make, model, procSpeed, salePrice) => ((make, model, procSpeed), salePrice)
    }

    val result = preparedLaptops.join(preparedOffers) // RDD[(make, model, procSpeed), (reg, salePrice))
      .map(_._2)
      .aggregateByKey((0.0, 0))(
        {
          case ((totalPrice, numPrices), salePrice) => (totalPrice + salePrice, numPrices + 1) // combine state with record
        },
        {
          case ((totalPrices1, numPrices1), (totalPrices2, numPrices2)) => (totalPrices1 + totalPrices2, numPrices1 + numPrices2) // combine 2 states into one
        }
      ) // RDD[(String, (Double, Int))]
      .mapValues {
        case (totalPrices, numPrices) => totalPrices / numPrices
      }

    result.count()
  }


  def main(args: Array[String]): Unit = {
//    plainJoin()
    noSkewJoin()
    Thread.sleep(1000000)
  }

  /**
   * Key points:
   * - Non uniform data distribution = data skews
   * - Can cause massive performance problems
   * -- Extremely long jobs
   * -- Sometiems can lead to excutor throwing OOM error/exception
   *
   * Solution: include extra information in the join key set
   * -- New key to join with
   * -- Redistribution of data by N+1 join keys
   * -- Uniform tasks
   *
   * Tip: Joining "Wide" RDDs is challenging, may be better to use the DataFrame API
   */

}
