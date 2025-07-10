package part2_foundations

import org.apache.spark.sql.SparkSession

/**
 * - Understand how Spark splits jobs into computational chunks
 * - Make the distinction between narrow and wide transformations
 * - Understand shuffle
 */

object SparkJobAnatomy {

  // Required for running locally in IntelliJ. Not required in Spark Shell
  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Spark Job Anatomy")
    .getOrCreate()

  val sc = spark.sparkContext

  //------------------Set up---------------------------

  /**
   * Cluster prep
   *
   * 1. Navigate to the spark-optimization folder, go to spark-cluster/
   * 2. docker-compose up --scale spark-worker=3
   * 3. In another terminal:
   *    - docker-exec -it spark-cluster_spark-master_1 bash
   *    - cd spark/bin
   *    - ./spark-shell
   * 4. In (yet) another terminal:
   *    - go to spark-optimization
   *    - docker cp (the data folder) spark-cluster_spark-master_1:/tmp
   * 5. Open http//:localhost:4040 for the Spark UI
   */

  // Example 1 - a count
  val rdd1 = sc.parallelize(1 to 1000000)
  rdd1.count
  // The result of this in the UI should be one stage with 6 tasks
  // Task = a unit of computation applied to a unit of data (a partition)

  // Example 2 - a count with a small transformation
  rdd1.map(_ * 2).count
  // The result of this in the UI is another job with one stage and 6 tasks
  // All parallelizable computations (like maps) are done in a single stage.

  // Example 3 - a count with a shuffle
  rdd1.repartition(23).count
  // Should see in the User Interface: 2 stages, one with 6 tasks, one with 23 tasks
  // Each stage is delimited by shuffles

  // Example 4 - a more complex computation: load a file and compute the average salary of the employees by department
  val employees = sc.textFile("/tmp/employees.csv")
  // process the lines
  val empTokens = employees.map(line => line.split(","))
  // extract relevant data
  val empDetails = empTokens.map(tokens => (tokens(4), tokens(7)))
  // group the elements
  val empGroups = empDetails.groupByKey(2)
  // process the values associated to each group
  val avgSalaries = empGroups.mapValues(salaries => salaries.map(_.toInt).sum / salaries.size)
  // show the result
  avgSalaries
    .collect() // this is an action
    .foreach(println)

  /**
   * Overview - TODO add diagrams
   * - Step 1: read the text file as a DF into 6 partitions
   * - Step 2: split the records into lines
   * ---> Narrow transformations: partitions don't need to know about each other
   * - Step 3: tuple the relevant information maps are narrow transformations
   * - Step 4: group the data by department
   * ---> Wide transformation: all partitions need to be considered (shuffled: data is moved in between executors VERY EXPENSIVE)
   * - Step 5: Average the values in each group
   * ---> Narrow transformation
   *
   * Stages and Tasks
   * - Task
   *  -- The smallest unit of computation
   *  -- Executed once, for one partition by one executor
   * - Stage
   *  -- Contains tasks
   *  -- Enforces no exchange of data and no partitions need data from other partitions
   *  -- Depends on the previous stage and the previous stage must complete before this one starts
   *  - Shuffle
   *  -- Exchange of data between executors
   *  -- Happens in between stages
   *  -- Must complete before next stage starts
   *  An application contains jobs
   *  A job contains stages
   *  A stage contains tasks
   *
   *  Dependencies
   *  - Narrow dependencies
   *  -- One input (parent) partition influences a single output (child) partition
   *  -- Fast to compute
   *  -- Examples: map, flatMap, filter, projections
   *  TODO - add diagram
   *  - Wide Dependencies
   *  -- One input partition influences more than once output partitions
   *  -- involve a shuffle i.e. data transfer between spark executors
   *  -- are costly to compute
   *  -- Examples: grouping, joining and sorting
   *  TODO - add diagram
   *  ---------------------------------------- Alternative definition
   *  - Dependencies
   *  -- Expressed differently in terms of "depends on"
   *  - Narrow dependencies
   *  -- Given a parent partition, a single child partition depends on it.
   *  -- Fast to compute
   *  -- Examples: map, flatMap, filter, projections
   *  - Wide dependencies
   *  -- Given a parent partition, more than one child partitions depend on it.
   *  -- Involve a shuffle that transfers data between Spark executors
   *  -- Are costly to compute
   *  -- Examples: Grouping, joining and sorting.
   *
   *
   *  - Shuffles
   *  -- Data exchanges between executors in the cluster
   *  -- Expensive because of
   *  --- Transferring data
   *  --- Serialisation/ deserialisation
   *  --- Loading new data from shuffle files
   *  -- Shuffles are performance bottlenecks because
   *  --- Exchanging data takes time
   *  --- They need to be fully completed before next computations start
   *  -- Shuffles limit parallelization
   *
   *
   */


}
