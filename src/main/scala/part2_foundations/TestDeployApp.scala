package part2_foundations

import org.apache.spark.sql.SparkSession

/**
 * - Packaging and shipping a Spark application to a cluster via Jars
 * - Cluster deploy modes
 * - Three ways of configuring Spark applications
 *
 *
 * Spark App Execution
 * - Selection of command line arguments to pass:
 * > --executor-memory --> allocates a certain amount of RAM/executor
 * > --driver-memoty --> allocate a certain amount of RAM for the driver
 * > --jars --> add additional JVM libraries for Spark to have access to
 * > --packages --> add addtional libraries such as Maven coordinates
 * > --conf (configName) (configValue): other configurations to the Spark application (including JVM options)
 * > --help: show all options
 *
 * The Anatomy of a Cluster
 * - Spark clsuter manager
 * --> one node manages the state of the cluster
 * --> the other do the work
 * --> communicate via driver/worker processes
 * - Standalone, YARN, Mesos, Kubernetes
 * - Spark Driver
 * --> Manages the state of the stages / tasks of the application
 * --> interfaces with the cluster manager
 * - Spark executors
 * --> Run the tasks assigned by the Spark driver
 * --> Report their state and results to the driver
 * Execution Mode
 * - Cluster
 * - Client
 * - Local
 * Cluster mode
 * - The spark driver is launched on a worker node
 * - the cluster manager is responsible for Spark processes
 * Client mode
 * - The spark driver is on the client machine
 * - The client is responsible for the Spark processes and state management
 * Local mode
 * - The entire application runs on the same machine
 *
 * Spark Cluster mode
 * - Driver is dedicated JVM container on the cluster
 * Pros
 * - Usually more memory availability for the driver
 * - Faster communication between driver and executors
 * - Faster performance overall
 * Cons
 * - Failure of node with the driver means application failed
 * - Fewer resources allocated to the executors
 *
 * Spark Client Mode
 * - Driver is created on the machine which submits the job
 * Pros
 * - More resources to the executors
 * - Node failures doesn't crash the application
 * - Results are immediately available on the machine
 * Cons
 * - Usually fewer resources available to the driver
 * - Communication overhead between the driver and the executors
 * - Very likely slower performance
 *
 */

object TestDeployApp {

  // TestDeployApp inputFile outputFile -- read some data from the input file and then write some data to the output file
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Need input file and output file")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("Test Deploy App")
      // method 1
      .config("spark.executor.memory", "1g") //One gig of memory
      .getOrCreate()

    import spark.implicits._

    val moviesDF = spark.read
      .option("inferSchema", "true")
      .json(args(0))

    val goodComediesDF = moviesDF.select(
        $"Title",
        $"IMDB_Rating".as("Rating"),
        $"Release_Date".as("Release")
      )
      .where(($"Major_Genre" === "Comedy") and ($"IMDB_Rating" > 6.5))
      .orderBy($"Rating".desc_nulls_last)

    // method 2
    spark.conf.set("spark.executor.memory", "1g") // warning - not all configurations available

    /*
      method 3: pass configs as command line arguments:

        spark-submit ... --conf spark.executor.memory 1g

      You can also use dedicated command line arguments for certain configurations:
        --master = spark.master
        --executor-memory = spark.executor.memory
        --driver-memory = spark.driver.memory

        and many more.
    */
    goodComediesDF.show()

    goodComediesDF.write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save(args(1))

  }

}
