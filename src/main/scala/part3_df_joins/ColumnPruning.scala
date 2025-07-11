package part3_df_joins

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, upper}

/**
 * - Understand a join optimisation done "out-of-the-box"
 * - Exploit pruning pushdown
 * - Optimise joins with pruning and map-side operation pushdown
 */

object ColumnPruning {

  val spark = SparkSession.builder()
    .appName("Column Pruning")
    .master("local[2]")
    .getOrCreate()

  val sc = spark.sparkContext
  import spark.implicits._

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars/guitars.json")

  val guitarPlayersDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands/bands.json")


  val joinCondition = guitarPlayersDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitarPlayersDF.join(bandsDF, joinCondition, "inner")
  guitaristsBandsDF.explain()

  /*
  == Physical Plan ==
  *(2) BroadcastHashJoin [band#22L], [id#38L], Inner, BuildLeft  //Optimal join
  :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true])), [id=#34]
  :  +- *(1) Project [band#22L, guitars#23, id#24L, name#25] <-- This step is unnecessary: Data frames already have these columns
  :     +- *(1) Filter isnotnull(band#22L)
  :        +- BatchScan[band#22L, guitars#23, id#24L, name#25] JsonScan Location: InMemoryFileIndex[file:spark-optimization/src/main/resources..., ReadSchema: struct<band:bigint,guitars:array<bigint>,id:bigint,name:string>
  +- *(2) Project [hometown#37, id#38L, name#39, year#40L]
     +- *(2) Filter isnotnull(id#38L)
        +- BatchScan[hometown#37, id#38L, name#39, year#40L] JsonScan Location: InMemoryFileIndex[file:spark-optimization/src/main/resources..., ReadSchema: struct<hometown:string,id:bigint,name:string,year:bigint>
   */

  val guitaristsWithoutBandsDF = guitarPlayersDF.join(bandsDF, joinCondition, "left_anti")
  guitaristsWithoutBandsDF.explain()
  /*
    == Physical Plan ==
    *(2) BroadcastHashJoin [band#22L], [id#38L], LeftAnti, BuildRight
    :- *(2) Project [band#22L, guitars#23, id#24L, name#25] <- UNNECESSARY
    :  +- BatchScan[band#22L, guitars#23, id#24L, name#25] JsonScan Location: InMemoryFileIndex[file:spark-optimization/src/main/resources..., ReadSchema: struct<band:bigint,guitars:array<bigint>,id:bigint,name:string>
    +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true])), [id=#66]
       +- *(1) Project [id#38L] <- This is "COLUMN PRUNING" - this has shrank the bands data frame so that it is just the id column
          +- *(1) Filter isnotnull(id#38L)
             +- BatchScan[id#38L] JsonScan Location: InMemoryFileIndex[file:spark-optimization/src/main/resources..., ReadSchema: struct<id:bigint>

    Column pruning = cut off columns that are not required
    ---- this shrinks DF and is very useful for joins and groups especially when the data frames involved are quite large
   */

  // project and filter pushdown
  val namesDF = guitaristsBandsDF.select(guitarPlayersDF.col("name"), bandsDF.col("name"))
  namesDF.explain()

  /*
  == Physical Plan ==
  *(2) Project [name#25, name#39]
  +- *(2) BroadcastHashJoin [band#22L], [id#38L], Inner, BuildLeft // The join step
     :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true])), [id=#100]
     :  +- *(1) Project [band#22L, name#25] <- COLUMN PRUNING before the join is performed
     :     +- *(1) Filter isnotnull(band#22L)
     :        +- BatchScan[band#22L, name#25] JsonScan Location: InMemoryFileIndex[file:spark-optimization/src/main/resources..., ReadSchema: struct<band:bigint,name:string>
     +- *(2) Project [id#38L, name#39]
        +- *(2) Filter isnotnull(id#38L)
           +- BatchScan[id#38L, name#39] JsonScan Location: InMemoryFileIndex[file:spark-optimization/src/main/resources..., ReadSchema: struct<id:bigint,name:string>

   - Spark tends to drop columns as early as possible. but it doesn't always do this automatically
   - Performing these queries this should be done manually as well.
   ----> Need to understand where Spark doesn't it automatically and when it needs to be done manually
   */

  val rockDF = guitarPlayersDF
    .join(bandsDF, joinCondition)
    .join(guitarsDF, array_contains(guitarPlayersDF.col("guitars"), guitarsDF.col("id")))

  val essentialsDF = rockDF.select(guitarPlayersDF.col("name"), bandsDF.col("name"), upper(guitarsDF.col("make")))
  essentialsDF.explain()

  /*
  == Physical Plan ==
  *(4) Project [name#25, name#39, upper(make#9) AS upper(make)#164] TODO the upper function is done LAST --- spark cannot optimise this but it can be done manually. See comments below
  +- BroadcastNestedLoopJoin BuildRight, Inner, array_contains(guitars#23, id#8L)
     :- *(2) Project [guitars#23, name#25, name#39]
     :  +- *(2) BroadcastHashJoin [band#22L], [id#38L], Inner, BuildLeft
     :     :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, true])), [id=#156]
     :     :  +- *(1) Project [band#22L, guitars#23, name#25] TODO <- Column pruning
     :     :     +- *(1) Filter isnotnull(band#22L)
     :     :        +- BatchScan[band#22L, guitars#23, name#25] JsonScan Location: InMemoryFileIndex[file:spark-optimization/src/main/resources..., ReadSchema: struct<band:bigint,guitars:array<bigint>,name:string>
     :     +- *(2) Project [id#38L, name#39] TODO <- Column pruning
     :        +- *(2) Filter isnotnull(id#38L)
     :           +- BatchScan[id#38L, name#39] JsonScan Location: InMemoryFileIndex[file:spark-optimization/src/main/resources..., ReadSchema: struct<id:bigint,name:string>
     +- BroadcastExchange IdentityBroadcastMode, [id=#167]
        +- *(3) Project [id#8L, make#9] TODO <- Column pruning
           +- BatchScan[id#8L, make#9] JsonScan Location: InMemoryFileIndex[file:spark-optimization/src/main/resources..., ReadSchema: struct<id:bigint,make:string>
   */

  /**
   * Key Point for manually optimising:
   * if you anticipate that the joined table is much larger than the table on whose column you are applying the
   * map-side operation, e.g. " * 5", or "upper", do this operation on the small table *FIRST*.
   *
   * This is particularly useful for outer joins.
   */

  def main(args: Array[String]): Unit = {

  }

  /**
   * Column Pruning
   * - Spark selects just the relevant columns after a join
   * - If you do a select after a join, the Project operation is pushed to joined DFs
   * - Further map-side operations can be manually pushed down
   * --- If we anticipate the joined DF is bigger than either side
   * - Spark sometimes can't prune columns automatically
   * --- Good practice: Select just the right columns ourselves before the join
   * - The performance benefits are especially seen in massive datasets
   */

}