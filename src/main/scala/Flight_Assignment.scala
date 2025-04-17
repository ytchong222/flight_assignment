import org.apache.log4j.{Level, Logger}//enable logging in the application
import org.apache.spark.sql.{DataFrame, SparkSession}//entry point to Spark functionality, enabling the reading and processing of data
import org.apache.spark.sql.expressions.Window//For advanced data operation
import org.apache.spark.sql.functions._//For advanced data operation
import org.apache.spark.sql.types._//For advanced data operation

import java.sql.Date//working with date values

object Flight_Assignment {

  // Create a logger for the application to print out the log/info to debug
  val log: Logger = Logger.getLogger(getClass.getName)

  // Initialize Spark session with performance tuning options
  lazy val spark: SparkSession = {
    SparkSession.builder()
      .appName("Flight Assignment")
      .master("local[*]") // Use all available cores
      .config("spark.sql.shuffle.partitions", "200") // Control shuffle scale for large datasets
      .config("spark.executor.memory", "4g")
      .config("spark.executor.cores", "2")
      .getOrCreate()
  }

  // Schema definitions for CSV files
  val flightDataSchema: StructType = StructType(Array(
    StructField("passengerId", IntegerType, nullable = false),
    StructField("flightId", IntegerType, nullable = false),
    StructField("from", StringType, nullable = false),
    StructField("to", StringType, nullable = false),
    StructField("date", StringType, nullable = false)
  ))

  val passengerSchema: StructType = StructType(Array(
    StructField("passengerId", IntegerType, nullable = false),
    StructField("firstName", StringType, nullable = false),
    StructField("lastName", StringType, nullable = false)
  ))
  /**
   * Validate schema to ensure input data matches the expected schema.
   */
  def validateSchema(df: DataFrame, schema: StructType): Unit = {
    val expectedColumns = schema.fields.map(_.name).toSet
    val actualColumns = df.schema.fields.map(_.name).toSet

    if (expectedColumns != actualColumns) {//error handling
      throw new IllegalArgumentException(s"Schema validation failed! Expected columns: ${expectedColumns.mkString(", ")}, found: ${actualColumns.mkString(", ")}")
    }
  }

  /**
   * Reusability to reads a CSV file into a DataFrame and validate schema.
   */
  def readCSV(filePath: String, schema: StructType)(implicit spark: SparkSession): DataFrame = {
    try {
      val df = spark.read
        .schema(schema)
        .option("header", "true")
        .csv(filePath)

      validateSchema(df, schema) // Reusability Validate schema for correctness to detect source data issues early
      df//return loaded and validated schema dataframe
    } catch {
      case e: Exception =>
        log.error(s"Error to read CSV at $filePath: ${e.getMessage}")
        throw new RuntimeException(e)
    }
  }
  /**
   * Question 1: Find the total number of flights for each month
   * This aggregates flight records by month to provide monthly counts.
   * @param flightData - DataFrame with flight data logs
   * @return DataFrame with two columns: Month and Number of Flights
   *         Summary Steps
   *         step 1: select only the required "date" column
   *         step 2: extract the month from the date
   *         step 3: group by month and count the number of flights
   *         step 4: order by month accending
   */


  def totalFlightsPerMonth(flightsDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    // Step 1: Select only the required "date" column
    val dateColumnDF = flightsDF.select("date")

    // Step 2: Extract the month from the "date" column
    val monthColumnDF = dateColumnDF.withColumn("Month", month(to_date(col("date"), "yyyy-MM-dd")))

    // Step 3: Group by "Month" and count the number of flights
    val groupedDF = monthColumnDF.groupBy("Month").count()

    // Step 4: Order results by "Month" in ascending order and rename count column
    groupedDF
      .select(col("Month"), col("count").as("Number of Flights"))
      .orderBy("Month")
  }

  /**
   * Question 2: Find the 100 most frequent flyers
   * @param flightData - DataFrame with flight info logs
   * @param passengers - DataFrame with customer details
   * @return DataFrame containing top 100 frequent flyers
   *        Summary steps
   *        Step 1: select the required columns( "flightData=passengerId" and "passengers='passengerId, firstName, lastName'")
   *        Step 2: aggregate flightData to count the number of flights for each passenger reduce size before join
   *        Step 3: broadcast the smaller/passenger dataset
   *        Step 4: select columns order and Rename it(Passenger ID",Number of Flights,First name,Last name
   *        Step 5: sort "Number of Flights" in descending order and limit 100 to get top 100 frequent flyers
   *
   */

  def findTopFlyersByFlightCount(flightData: DataFrame, passengers: DataFrame)(implicit spark: SparkSession): DataFrame = {

    // Step 1: Select only the necessary columns from flightData and passengers
    val selectedFlightData = flightData.select("passengerId")
    val selectedPassengersData = passengers.select("passengerId", "firstName", "lastName")

    // Step 2: Aggregate flightData to count the number of flights for each passenger reduce size before join
    val flightCounts = selectedFlightData
      .groupBy("passengerId") //group by passengerId
      .agg(count("*").as("Number of Flights"))// aggregate Number of Flights

    //Step 3: Broadcast the smaller/passenger dataset
    val frequentFlyers = flightCounts
      .join(broadcast(selectedPassengersData), "passengerId") // Broadcast the smaller/passenger dataset
      .select(
        col("passengerId").as("Passenger ID"),        // Rename passengerId to "Passenger ID"
        col("Number of Flights"),                    //  Remain "Number of Flights"
        col("firstName").as("First name"),           //  Rename firstName to "First name"
        col("lastName").as("Last name")              //  Rename lastName to "Last name"
      )
      .orderBy(desc("Number of Flights")) // Sort in descending order
      .limit(100) // Limit the output to top 100 frequent flyers
    //debuging
    //log.info("Top 10 Frequent Flyers: " + frequentFlyers.show(10, truncate = false))
    frequentFlyers  //return the data
  }
  /**
   * Question 3: Find the greatest number of countries a passenger has visited without visiting the UK.
   * @param flightData - DataFrame with flight info logs.
   * @return The maximum number of countries visited by any passenger, excluding the UK.
   *         Summary steps
   *         step 1: select only necessary columns to start with ("passengerId", "to", "date")
   *         Step 2: define a window for passenger flight data ordered by date
   *         Step 3: calculate the "RunId" to add constant value to "UK" to filter out "UK"
   *         Step 4: aggregate the longest run for each unique "RunId" and "passengerId"
   *         Step 5: find the maximum "RunLength" for each passenger and order by Longest Run desc
   *
   */
  def longestRunWithoutUK(flightData: DataFrame)(implicit spark: SparkSession): DataFrame = {
    // Step 1: Select only necessary columns to start with
    val reducedFlightData = flightData.select("passengerId", "to", "date")
                                      .withColumnRenamed("passengerId", "Passenger ID")//Rename "passengerId" to "Passenger ID"

    // Step 2: Define a window for passenger flight data ordered by date ascending
    val windowSpec = Window.partitionBy("Passenger ID") //group by passengerId
                           .orderBy("date") //order by date in ascending order

    // Step 3:Calculate the "RunId" to add constant value 1 to "UK" and filter out "UK"
    val runs = reducedFlightData
      .withColumn(
        "IsNewRun", // Assign 1 when the destination is "UK", otherwise 0
        when(col("to") === "UK", lit(1)).otherwise(lit(0))
      )
      .withColumn(
        "RunId", // Use a running sum to assign a unique "RunId" for uninterrupted sequences
        sum("IsNewRun").over(windowSpec)
      )
      .filter(col("to") =!= "UK") // Exclude rows where the destination is "UK" early to reduce rows

    // Step 4: Aggregate the longest run for each unique "RunId" and "passengerId"
    val runLengths = runs
      .groupBy("Passenger ID", "RunId")
      .agg(countDistinct("to").as("RunLength")) // Count distinct destinations in each uninterrupted run

    // Step 5: Find the maximum "RunLength" for each passenger and order by Longest Run desc
    val longestRun = runLengths
      .groupBy("Passenger ID")
      .agg(max("RunLength").as("Longest Run")) // Aggregate the longest uninterrupted run for each passenger
      .orderBy(desc("Passenger ID")) //Order by "Passenger ID" descending order

    longestRun//return data

  }

  /**
   * Question 4: Find pairs of passengers who have been on more than 3 flights together.
   * @param flightData - DataFrame with flight info logs.
   * @return DataFrame with unique passenger pairs (passengerId, Passenger2Id)
   *         and the number of flights they shared (Number of Flights Together),
   *         including only pairs with more than 3 shared flights.
   *         Summary steps
   *         Step 1: select only the necessary columns BEFORE the join ("flightId", "passengerId")
   *         Step 2: join the data on flightId, create passenger pairs
   *         step 3: filter out reverse duplicate and same passengerID
   *         step 4: group by passenger pairs and count their flights together
   *         Step 5: select and rename columns and sort by the number of shared flights desc
   */

  def flightsTogether(flightData: DataFrame)(implicit spark: SparkSession): DataFrame = {
    // Step 1: select only the necessary columns BEFORE the join
    val selectedColumns = flightData.select("flightId", "passengerId").repartition(col("flightId"))

    // Step2: join the data on flightId,create passenger pairs
    val passengerPairs = selectedColumns
      .alias("df1") // Alias for the left-side DataFrame
      .join(
        selectedColumns.withColumnRenamed("passengerId", "Passenger2Id").alias("df2"),
        col("df1.flightId") === col("df2.flightId") // Join on flightId
      )
      //step 3: filter out reverse duplicate and same passengerID
      .filter(col("df1.passengerId") < col("df2.Passenger2Id")) // Ensure unique pairs (avoid duplicates)

      //step 4: group by passenger pairs and count their flights together
      .groupBy("df1.passengerId", "df2.Passenger2Id")
      .agg(count("*").as("Number of Flights Together"))
      .filter(col("Number of Flights Together") > 3) // Filter pairs with >3 shared flights

    //step 5: select and rename columns and sort by the number of shared flights desc
    val result = passengerPairs
      .select(
        col("df1.passengerId").as("Passenger 1 ID"),
        col("df2.Passenger2Id").as("Passenger 2 ID"),
        col("Number of Flights Together")
      )
      .orderBy(desc("Passenger 1 ID"), desc("Passenger 2 ID")) // Sort by the "Passenger 1 ID","Passenger 2 ID" in descending order

    //debugging
    //log.info("select 10 passenger pairs:\n" + result.show(10, truncate = false))

    result//return data
  }

  /**
   * Finds pairs of passengers who have been on more than N flights together within a specific date range.
   * @param flightData - DataFrame with flight info logs
   * @param atLeastNTimes Minimum number of flights the passenger pairs should have fly together
   * @param from Start date for the range
   * @param to End date for the range
   * @return passenger pairs, number of flights, and  flight range
   *         Summary steps
   *         Step 1: filter flights within the date range and repartition and for better self-join performance
   *         Step 2: separate the logic and Perform the self-join to identify passenger pairs
   *         Step 3: group by passenger pairs and perform aggregations
   *         Step 4: format and order the results column (Passenger 1 ID,Passenger 2 ID,Number of Flights Together,From,To)
   *         step 5: order by "From" and "To" in ascending order
   */
  def flownTogether(flightData: DataFrame, atLeastNTimes: Int, from: Date, to: Date)(implicit spark: SparkSession): DataFrame = {
    // Step 1: filter flights within the date range
    val filteredFlights = flightData
      .filter(col("date").between(from, to))
      .select("flightId", "passengerId","date")// selected column
      .repartition(col("flightId")) // Repartition for better self-join performance

    //debugging
    //println("=== filteredFlights===")
    //log.info("show filteredFlights:\n" + filteredFlights.show(10, truncate = false))

    // Step 2: separate the logic and Perform the self-join to identify passenger pairs on the same flights
    val passengerPairs = filteredFlights
      .alias("df1")
      .join(
        filteredFlights
          .withColumnRenamed("passengerId", "Passenger2Id")
          .as("df2"),
        col("df1.flightId") === col("df2.flightId")
      )//self-join to identify passenger pairs on the same flights

      .filter(col("df1.passengerId") < col("df2.Passenger2Id"))//To exclude df1.passengerId == df2.Passenger2I (P1=P1)
    //debugging
    //println("=== passengerPairs===")
    //log.info("show passengerPairs:\n" + passengerPairs.show(10, truncate = false))
    // Step 3: group by passenger pairs and perform aggregations
    val groupedPairs = passengerPairs
      .groupBy(
        col("df1.passengerId").as("Passenger 1 ID"),//Group by passenger 1 ID
        col("df2.Passenger2Id").as("Passenger 2 ID")//Group by passenger 2 ID
      )
      .agg(
        count(col("df1.flightId")).as("Number of Flights Together"),// Count the number of flights together
        min(col("df1.date")).as("From"),// The earliest flight date the pair traveled together

        max(col("df1.date")).as("To")  //The lastest flight date the pair traveled together
      )
      .filter(col("Number of Flights Together") > atLeastNTimes)// Filter where they flew at least N times together
    //debugging
    // println("=== groupedPairs===")
    //log.info("show groupedPairs:\n" + groupedPairs.show(10, truncate = false))

    // Step 4: format and order the results
    groupedPairs
      .select(
        col("Passenger 1 ID"),
        col("Passenger 2 ID"),
        col("Number of Flights Together"),
        date_format(col("From"), "yyyy-MM-dd").as("From"),// Date format 'yyyy-mm-dd'
        date_format(col("To"), "yyyy-MM-dd").as("To")     // Date format 'yyyy-mm-dd'
      )//select the column order to display
      .orderBy(asc("From"), asc("To")) // Sort by the "From","to" in ascending order

  }

  def main(args: Array[String]): Unit = {
    implicit val sparkSession: SparkSession = spark

    val flightDataPath = "data/flightData.csv"
    val passengersPath = "data/passengers.csv"

    // Read data and avoid recomputation and allow spilling to disk if memory is limited
    val flightData = readCSV(flightDataPath, flightDataSchema).persist()
    val passengers = readCSV(passengersPath, passengerSchema).persist()

    // Debug source file
//    log.info(s"Number of rows in flightData: ${flightData.count()}")
//    flightData.orderBy(asc("passengerId")).show(10)
//
//    log.info(s"Number of rows in passengers: ${passengers.count()}")
//    passengers.orderBy(asc("passengerId")).show(10)

    log.info("Question 1: Total Flights Per Month:")
    totalFlightsPerMonth(flightData).show()


    log.info("Question 2: 100 Most Frequent Flyers:")
    findTopFlyersByFlightCount(flightData, passengers).show(100, truncate = false)


    log.info("Question 3: Longest Run Without Visiting UK:")
    longestRunWithoutUK(flightData).show()

    // Question 4: Passengers on More than 3 Flights Together

    log.info("Question 4: Passengers on More Than 3 Flights Together:")
    flightsTogether(flightData).show()

    // Passengers on N+ Flights Together
    val fromDate = Date.valueOf("2017-01-01")
    val toDate = Date.valueOf("2017-12-31")
    log.info("Passengers on More Than N Flights Together Within Date Range:")
    flownTogether(flightData, atLeastNTimes = 3, fromDate, toDate).show()

    // free up the memory after use
    flightData.unpersist()
    passengers.unpersist()
  }
}