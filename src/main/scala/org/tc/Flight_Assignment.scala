package org.tc
import org.apache.spark.sql.{Dataset, SparkSession}//entry point to Spark functionality, enabling the reading and processing of data
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}//enable logging in the application

import java.sql.Date                   //working with date values for database
import java.time.LocalDate             //general purpose date manipulation and comparison
import org.apache.spark.sql.types._

// Define all case classes for dataset column and data type
case class FlightData(passengerId: Int, flightId: Int, from: String, to: String, date: String)
case class Passenger(passengerId: Int, firstName: String, lastName: String)
case class MonthlyFlights(Month: Int, `Number of Flights`: Long)
case class FrequentFlyer(`Passenger ID`: Int, `Number of Flights`: Long, `First name`: String, `Last name`: String)
case class LongestRunResult(`Passenger ID`: Int, `Longest Run`: Long)
case class FlightsTogetherResult(`Passenger 1 ID`: Int, `Passenger 2 ID`: Int, `Number of flights together`: Long)
case class FlightsTogetherWithRange(`Passenger 1 ID`: Int, `Passenger 2 ID`: Int, `Number of flights together`: Long,  From: Date, To: Date)

object Flight_Assignment {
  val log: Logger = Logger.getLogger(getClass.getName)
  // Creating a Spark session
  val spark: SparkSession = SparkSession.builder()
    .appName("Flight Statistics")
    .master("local[*]") // Adjust cluster mode if needed
    .getOrCreate()

  import spark.implicits._

  /**
   * Define schemas for dataset validation
   */
  val flightSchema: StructType = StructType(Seq(
    StructField("passengerId", IntegerType, nullable = true),
    StructField("flightId", IntegerType, nullable = true),
    StructField("from", StringType, nullable = true),
    StructField("to", StringType, nullable = true),
    StructField("date", StringType, nullable = true)
  ))

  val passengerSchema: StructType = StructType(Seq(
    StructField("passengerId", IntegerType, nullable = true),
    StructField("firstName", StringType, nullable = true),
    StructField("lastName", StringType, nullable = true)
  ))


  /**
   * Utility function to validate schema
   */
  def validateSchema(expectedSchema: StructType, actualSchema: StructType): Unit = {
    if (expectedSchema != actualSchema) {
      throw new IllegalArgumentException(
        s"Schema mismatch! \nExpected: $expectedSchema\nActual: $actualSchema"
      )
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
  def totalFlightsPerMonth(flights: Dataset[FlightData]): Dataset[MonthlyFlights] = {
    import flights.sparkSession.implicits._

    // Step 1:
    val dateColumn = flights.select("date")

    // Step 2:
    val monthColumn = dateColumn.withColumn("Month", month(to_date(col("date"), "yyyy-MM-dd")))

    // Step 3:
    val groupedFlights = monthColumn.groupBy("Month").agg(count("*").as("Number of Flights"))
    //debuging
    //log.info("groupedFlights: " + groupedFlights.show(10, truncate = false))
    // Step 4:
    groupedFlights
      .select("Month", "Number of Flights") // Ensure the output columns are properly named and ordered
      .as[MonthlyFlights] // Convert to the case class Dataset[MonthlyFlights]
      .orderBy("Month") // Order by Month in ascending order

  }

  /**
   * Question 2: Find the 100 most frequent flyers
   * @param flightData - DataFrame with flight info logs
   * @param passengers - DataFrame with customer details
   * @return DataFrame containing top 100 frequent flyers
   *        Summary steps
   *        Step 1: select the required columns( "flightData=passengerId" and "passengers='passengerId, firstName, lastName'")
   *        Step 2: aggregate flightData to count the number of flights for each passenger reduce size before join
   *        Step 3: join broadcast the smaller/passenger dataset and  select columns order and Rename it(Passenger ID",Number of Flights,First name,Last name
   *        Step 4: sort "Number of Flights" in descending order and limit 100 to get top 100 frequent flyers
   *
   */
  def mostFrequentFlyers(flights: Dataset[FlightData], passengers: Dataset[Passenger]): Dataset[FrequentFlyer] = {
    import flights.sparkSession.implicits._

    // Step 1:
    val selectedFlights = flights.select("passengerId", "flightId") // Keep only passengerId and flightId
    val selectedPassengers = passengers.select("passengerId", "firstName", "lastName") // Keep passengerId, firstName, and lastName

    // Step 2:
    val aggregatedFlights = selectedFlights
      .groupBy("passengerId")
      .agg(count("flightId").as("Number of Flights"))

    // Step 3:
    val result = aggregatedFlights
      .join(broadcast(selectedPassengers), "passengerId") // Join on passengerId
      .select(
        col("passengerId").as("Passenger ID"),      // Rename passengerId to Passenger ID
        col("Number of Flights"),                   // Include Number of Flights as is
        col("firstName").as("First name"),         // Rename firstName to First Name
        col("lastName").as("Last name")          // Rename lastName to Last Name

      )
      .as[FrequentFlyer] // Map to FrequentFlyer case class

      // Step 4:
      .orderBy(desc("Number of Flights"))
      .limit(100)

    result
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
  def longestRunWithoutUK(flights: Dataset[FlightData]): Dataset[LongestRunResult] = {
    import flights.sparkSession.implicits._

    // Step 1:
    val reducedFlights = flights
      .select("passengerId", "to", "date") // Only keep the relevant columns
      .withColumnRenamed("passengerId", "Passenger ID") // Rename `passengerId` to `Passenger ID`
    //debuging
    //reducedFlights.show()
    //log.info("reducedFlights:\n" + reducedFlights.show(18, truncate = false))
    // Step 2:
    val windowSpec = Window.partitionBy("Passenger ID").orderBy("date") // Group by `Passenger ID` and sort by date

    // Step 3:
    val flightsWithRunId = reducedFlights
      .withColumn(
        "wasPreviousUK", // Determine if the previous destination was "UK"
        lag("to", 1).over(windowSpec).equalTo("UK").cast("int")
      )
      .withColumn(
        "RunId", // Increment run ID cumulatively where the previous destination was "UK"
        sum("wasPreviousUK").over(windowSpec)
      )
      .filter(col("to") =!= "UK") // Exclude flights where destination is "UK"
    // Step 4:
    val runLengths = flightsWithRunId
      .groupBy(col("Passenger ID"), col("RunId")) // Group by Passenger ID and Run ID
      .agg(count("*").as("RunLength")) // Count destinations in each uninterrupted run

    // Step 5:
    val longestRun = runLengths
      .groupBy(col("Passenger ID")) // Group by Passenger ID
      .agg(max("RunLength").as("Longest Run")) // Find the maximum run length for each passenger
      .orderBy(desc("Longest Run")) // Order passengers by their longest run in descending order

      longestRun.as[LongestRunResult] // Return as Dataset[LongestRunResult]
  }
//
  /**
   * Question 4: Find pairs of passengers who have been on more than 3 flights together.
   * @param flightData - DataFrame with flight info logs.
   * @return DataFrame with unique passenger pairs (passengerId, Passenger2Id)
   *         and the number of flights they shared (Number of Flights Together),
   *         including only pairs with more than 3 shared flights.
   *         Summary steps
   *         Step 1: select only the necessary columns BEFORE the join ("flightId", "passengerId","date")
   *         Step 2: join the data on flightId and date create passenger pairs and filter out reverse duplicate and same passengerID
   *         step 3: group by passenger pairs and count their flights together
   *         Step 4: select and rename columns and sort by the number of shared flights desc
   */
  def flightsTogether(flights: Dataset[FlightData]): Dataset[FlightsTogetherResult] = {
    import flights.sparkSession.implicits._

    // Step 1: Select only necessary columns and repartition by `flightId`
    val selectedColumns = flights.select("flightId", "passengerId","date").repartition(col("flightId"))

    // Step 2: Join the data on `flightId`, creating passenger pairs
    val passengerPairs = selectedColumns
      .alias("df1") // Alias for the left DataFrame
      .join(
        selectedColumns.withColumnRenamed("passengerId", "Passenger2Id").alias("df2"), // Alias for the right DataFrame
        col("df1.flightId") === col("df2.flightId") && col("df1.date") ===col("df2.date")// Join on `flightId` and date
      )
      .filter(col("df1.passengerId") < col("df2.Passenger2Id")) // Ensure unique pairs (passengerId < Passenger2Id)
    //debuging
   //    passengerPairs.show()
   //    log.info("passengerPairs:\n" + passengerPairs.show(10, truncate = false))

    // Step 3:
    val groupedPairs = passengerPairs
      .groupBy("df1.passengerId", "df2.Passenger2Id") // Group by the two passenger IDs
      .agg(count("*").as("Number of Flights Together")) // Count the number of shared flights
      .filter(col("Number of Flights Together") > 3) // Filter out pairs with <= 3 shared flights

    // Step 4:
    val result = groupedPairs
      .select(
        col("df1.passengerId").as("Passenger 1 ID"),              // Rename `passengerId` to `Passenger 1 ID`
        col("df2.Passenger2Id").as("Passenger 2 ID"),             // Rename `Passenger2Id` to `Passenger 2 ID`
        col("Number of Flights Together") // Keep column as-is for output
      )
      .orderBy(desc("Number of Flights Together")) // Sort by number of flights together in descending order

    // Step 5: Map to `FlightsTogetherResult` case class
    result.as[FlightsTogetherResult]

  }

  /**
   * Extra Task:Finds pairs of passengers who have been on more than N flights together within a specific date range.
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
  def flownTogetherWithinRange(
                                flights: Dataset[FlightData],
                                atLeastNTimes: Int,
                                from: Date,
                                to: Date
                              ): Dataset[FlightsTogetherWithRange] = {
    import flights.sparkSession.implicits._

    // Step 1:
    val filteredFlights = flights
      .filter(col("date").between(from, to)) // Filter rows for the specified date range
      .select("flightId", "passengerId", "date") // Select only relevant columns
      .repartition(col("flightId")) // Repartition by `flightId` for optimized join performance

    // Step 2:
    val passengerPairs = filteredFlights
      .alias("df1")
      .join(
        filteredFlights
          .withColumnRenamed("passengerId", "Passenger2Id")
          .alias("df2"),
        col("df1.flightId") === col("df2.flightId") // Join on `flightId`
      )
      .filter(col("df1.passengerId") < col("df2.Passenger2Id")) // Ensure unique pairs (ascending order)

    // Step 3:
    val groupedPairs = passengerPairs
      .groupBy(
        col("df1.passengerId").as("Passenger 1 ID"),
        col("df2.Passenger2Id").as("Passenger 2 ID")
      )
      .agg(
        count(col("df1.flightId")).as("Number of Flights Together"), // Count the number of flights together
        min(col("df1.date")).as("From"), // Earliest date of the flights together
        max(col("df1.date")).as("To") // Latest date of the flights together
      )
      .filter(col("Number of Flights Together") > atLeastNTimes) // Filter pairs flying together more than the threshold

    // Step 4-5:
    groupedPairs
      .select(
        col("Passenger 1 ID"),
        col("Passenger 2 ID"),
        col("Number of Flights Together"),
        date_format(col("From"), "yyyy-MM-dd").as("From"), // Format `from` date as yyyy-MM-dd
        date_format(col("To"), "yyyy-MM-dd").as("To") // Format `to` date as yyyy-MM-dd
      )
      .orderBy(asc("From"), asc("To")) // Sort by `From` and `To` dates in ascending order
      .as[FlightsTogetherWithRange] // Map to the case class
  }
  /**
   * Main function. Executes all analysis tasks and saves output as CSV files.
   */
  def main(args: Array[String]): Unit = {

    // Define paths for input files and output directory
    val flightDataPath = "data/flightData.csv"
    val passengersPath = "data/passengers.csv"
    val outputDirectory = "data/output" // Define output directory for CSV files

    // Read the CSV files into typed Datasets
    val flightData: Dataset[FlightData] = spark.read
      .option("header", "true")
      .schema(flightSchema)
      .csv(flightDataPath)
      .as[FlightData]
      .persist()

    val passengers: Dataset[Passenger] = spark.read
      .option("header", "true")
      .schema(passengerSchema)
      .csv(passengersPath)
      .as[Passenger]
      .persist()

    // Validate schema
    validateSchema(flightSchema, flightData.schema)
    validateSchema(passengerSchema, passengers.schema)

    // Question 1
    val totalFlights = totalFlightsPerMonth(flightData)
   // val tableTotalFlights = totalFlights.show(12, truncate = false) // Fetch first 12 rows as a string
    // log.info("tableTotalFlights:\n" + tableTotalFlights) // Log the table
    totalFlights.write.mode("overwrite").option("header", "true").csv(s"$outputDirectory/Q1_totalFlights")

    // Question 2
    val frequentFlyers = mostFrequentFlyers(flightData, passengers)
    //val tableRequentFlyers = frequentFlyers.show(100, truncate = false) // Fetch first 100 rows as a string
    // log.info("tableRequentFlyers:\n" + tableRequentFlyers) // Log the table
    frequentFlyers.write.mode("overwrite").option("header", "true").csv(s"$outputDirectory/Q2_frequentFlyers")

    // Question 3
    val longestRun = longestRunWithoutUK(flightData)
    // val tablelongestRunWithoutUK = longestRun.show(10, truncate = false) // Fetch first 10 rows as a string
    //log.info("tablelongestRunWithoutUK:\n" + tablelongestRunWithoutUK) // Log the table

    longestRun.write.mode("overwrite").option("header", "true").csv(s"$outputDirectory/Q3_longestRun")

       // Question 4
      val togetherMoreThan3 = flightsTogether(flightData)
//      val tabletogetherMoreThan3 = togetherMoreThan3.show(10, truncate = false) // Fetch first 10 rows as a string
//      log.info("tablelongestRunWithoutUK:\n" + tabletogetherMoreThan3) // Log the table
     togetherMoreThan3.write.mode("overwrite").option("header", "true").csv(s"$outputDirectory/Q4_flightsTogether")

    // Extra Task
    val fromDate = Date.valueOf(LocalDate.parse("2017-01-01"))
    val toDate = Date.valueOf(LocalDate.parse("2017-12-31"))
    val togetherInRange = flownTogetherWithinRange(flightData, 3, fromDate, toDate)

//    val tableTogetherInRange = togetherInRange.show(10, truncate = false) // Fetch first 10 rows as a string
//    log.info("tableTogetherInRange:\n" + tableTogetherInRange) // Log the table

    togetherInRange.write.mode("overwrite").option("header", "true").csv(s"$outputDirectory/Extra_flightsTogetherInRange")

    // free up the memory after use
    flightData.unpersist()
    passengers.unpersist()
    log.info("All tasks completed. CSV files saved in the output directory.")
  }
}