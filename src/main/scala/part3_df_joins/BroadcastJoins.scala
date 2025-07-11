package part3_df_joins

import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * - Joining a large DataFrame with a small DataFrame
 * --> technique: broadcasting -- improves performance
 *
 * Large-Small Join
 * - Solution to performance issues: Broadcast join
 * - Smaller DF/RDD is sent to all executors
 * - All joins are done in memory
 * - Very little overhead - TODO see query plan - see BroadCaseExchange step
 *
 *
 * Broadcast Join
 * - Scenario: join with a lookup table
 * - Share the smaller DF/RDD across all executors
 * ---- Tiny overhead
 * ---- All other operations are done in memory
 * - Pros
 * --- Shuffles avoided
 * --- Very very fast
 * - Risks
 * --- Not enough driver memeory
 * --- If the smaller DF is quite big there is a large overhead
 * --- If the smaller DF is quite bug there is a risk of Out Of Memory errors for the executors
 * - Broadcasting can be done automatically by Spark
 * --- when it finds one DF smaller than a threshold
 * ------> this is only for DataFrames
 *
 */

object BroadcastJoins {

  val spark = SparkSession.builder()
    .appName("Broadcast Joins")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext

  val rows = sc.parallelize(List(
    Row(0, "zero"),
    Row(1, "first"),
    Row(2, "second"),
    Row(3, "third")
  ))

  // Defining a schema
  val rowsSchema = StructType(Array(
    StructField("id", IntegerType),
    StructField("order", StringType)
  ))

  // small table
  val lookupTable: DataFrame = spark.createDataFrame(rows, rowsSchema)

  // large table
  val table = spark.range(1, 100000000) // column is "id"

  // the potentially dangerous join
  //Why? --- Two shuffles (on 100,000,000 elements)
  val joined = table.join(lookupTable, "id")
  joined.explain
  // joined.show - takes a longer than required

  // a smarter join --- using broadcast
  val joinedSmart = table.join(broadcast(lookupTable), "id")
  joinedSmart.explain()
  // joinedSmart.show() // results in milliseconds to run the job

  // auto-broadcast detection (in the catalyst query optimiser)
  val bigTable = spark.range(1, 100000000)
  val smallTable = spark.range(1, 10000) // size estimated by Spark - auto-broadcast
  val joinedNumbers = smallTable.join(bigTable, "id")

  // deactivate auto-broadcast
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  joinedNumbers.explain()

  def main(args: Array[String]): Unit = {
    Thread.sleep(1000000)
  }

}
