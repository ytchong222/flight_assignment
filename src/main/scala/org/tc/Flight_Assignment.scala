package org.tc
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

import java.sql.Date
import java.time.LocalDate
import org.apache.spark.sql.types._

import java.time.format.DateTimeFormatter
import scala.util.Try

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
   * @return with two columns: Month and Number of Flights
   *         Summary Steps
   *         step 1: Group by flightId and date
   *         step 2: Extract month from date
   *         step 3: Group by month and count
   *         step 4: Map to case class
   *         step 5: Sort and return
   */
  def totalFlightsPerMonth(flights: Seq[FlightData]): Seq[MonthlyFlights] = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

//    println("Source Flights:")
//    flights.foreach(flight =>
//      println(s"Flight ID: ${flight.flightId}, Date: ${flight.date}, Passenger ID: ${flight.passengerId}")
//    )

    // Step 1:

    val distinctFlights: Seq[FlightData] = flights
      .groupBy(flight => (flight.flightId, flight.date)) // Group by flightId and date
      .map(_._2.head)                                   // Take the first flight in each group
      .toSeq                                            // Convert back to Seq

    //distinctFlights.foreach(println)

    // Step 2:
    val months: Seq[Int] = distinctFlights.flatMap { flight =>
      Try(LocalDate.parse(flight.date, formatter).getMonthValue).toOption
    }

    // Step 3:
    val groupedByMonth: Map[Int, Int] = months.groupBy(identity).map {
      case (month, flightsInMonth) => month -> flightsInMonth.size
    }

//    println("Grouped By Month:")
//    groupedByMonth.foreach {
//      case (month, count) => println(s"Month: $month -> Number of Flights: $count")
//    }

    // Step 4:
    val monthlyFlights: Seq[MonthlyFlights] = groupedByMonth.map {
      case (month, count) => MonthlyFlights(month, count.toLong)
    }.toSeq


//    monthlyFlights.foreach { flight =>
//      println(s"Month: ${flight.Month}, Number of Flights: ${flight.`Number of Flights`}")
//    }

    // Step 5: Sort and return
    monthlyFlights.sortBy(_.`Month`)
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
    selectedFlights.show()
    log.info("selectedFlights:\n" + selectedFlights.show(100, truncate = false))

    selectedPassengers.show()
    log.info("selectedPassengers:\n" + selectedPassengers.show(100, truncate = false))
    // Step 2:
    val aggregatedFlights = selectedFlights
      .groupBy("passengerId")
      .agg(count("flightId").as("Number of Flights"))
      .orderBy(desc("Number of Flights"))
      .limit(100)

    aggregatedFlights.show()
    log.info("selectedPassengers:\n" + aggregatedFlights.show(100, truncate = false))
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



    result.show()
    log.info("result:\n" + result.show(100, truncate = false))
    result
  }

  /**
   * Question 3: Find the greatest number of countries a passenger has visited without visiting the UK.
   * @param flightData - DataFrame with flight info logs.
   * @return The maximum number of countries visited by any passenger, excluding the UK.
   *         Summary steps
   *         step 1:  Select only the relevant columns and combine(from and to) into one column
   *         Step 2:  order by passengerId and date
   *         Step 3:  Group by passengerId
   *         Step 4: calculate longest run excluding UK and no duplicate consecutive countries
   *                 -if country =UK reset to 0 and assign prevCountry to UK and Update `maxRun` in case the current run ends here
   *                 -if current country = previous country do no nothing
   *                 -if <> UK and <> prevoius country increment 1,get the max count from maxRun/currentRun,
   *                    assign previous country to current country
   *         Step 5: return passengerId and the maxrun count
   *
   *
   */

  def longestRunWithoutUK(flights: Seq[FlightData]): Seq[LongestRunResult] = {
    // Step 1:
    val flightCountries = flights.flatMap(flight =>
      Seq(
        (flight.passengerId, flight.date, flight.from),
        (flight.passengerId, flight.date, flight.to)
      )
    )
    // Debugging
//    println(" flightCountries:")
//    flightCountries.foreach { case (passengerId, date, country) =>
//      println(s"Passenger: $passengerId, Date: $date, Country: $country")
//    }
    // Step 2:
    val sortedFlightCountries = flightCountries
      .sortBy { case (passengerId, date, _) => (passengerId, date) }

//    println(" sortedFlightCountries:")
//    sortedFlightCountries.foreach { case (passengerId, date, country) =>
//      println(s"Passenger: $passengerId, Date: $date, Country: $country")
//    }


    // Step 3:
    val countriesByPassenger = sortedFlightCountries.groupBy(_._1)//group by the passenger
    // Debugging
//    println("Group countries by passenger:")
//    countriesByPassenger.foreach { case (passengerId, trips) =>
//      println(s"Passenger ID: $passengerId")
//      trips.foreach { case (_, date, country) =>
//        println(s"  Date: $date, Country: $country")
//      }
//    }
    // Step 4:
    val longestRuns = countriesByPassenger.map { case (passengerId, trips) =>
      // Get ordered sequence of countries for this passenger
      val countrySequence = trips.map(_._3)

      var prevCountry: Option[String] = None
      var currentRun = 0L //define as long
      var maxRun = 0L //define as long

      for (country <- countrySequence) {
        // Debug: print current state before logic
        //println(s"passengerId:$passengerId | current country: $country | Previous: ${prevCountry.getOrElse("None")} | CurrentRun: $currentRun | MaxRun: $maxRun")

        //if country =UK reset to 0
        if (country == "UK") {
          currentRun = 0
          // Update `maxRun` in case the current run ends here
          maxRun = math.max(maxRun, currentRun)
          prevCountry = Some("UK") //Assign prevCountry to UK
        } else if (prevCountry.contains(country)) {
          //  no increment due to same country with previous country
        } else {
          currentRun += 1 // if <> UK and <> prevoius country increment 1
          maxRun = math.max(maxRun, currentRun) //the max count from maxRun/currentRun
          prevCountry = Some(country) //Assign previous country to current country
        }
        println(s"passengerId:$passengerId | current country: $country | Previous: ${prevCountry.getOrElse("None")} | CurrentRun: $currentRun | MaxRun: $maxRun")

      }

      LongestRunResult(passengerId, maxRun)// return passengerId and the maxrun count
    }
   //Step 5
    longestRuns.toSeq
  }




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
    //debuging
        selectedColumns.show()
        log.info("selectedColumns:\n" + selectedColumns.show(200, truncate = false))

    // Step 2: Join the data on `flightId`, creating passenger pairs
    val passengerPairs = selectedColumns
      .alias("df1") // Alias for the left DataFrame
      .join(
        selectedColumns.withColumnRenamed("passengerId", "Passenger2Id").alias("df2"), // Alias for the right DataFrame
        col("df1.flightId") === col("df2.flightId") && col("df1.date") ===col("df2.date")// Join on `flightId` and date
      )
      .filter(col("df1.passengerId") < col("df2.Passenger2Id")) // Ensure unique pairs (passengerId < Passenger2Id)
    //debuging
//        passengerPairs.show()
//        log.info("passengerPairs:\n" + passengerPairs.show(100, truncate = false))

    // Step 3:
    val groupedPairs = passengerPairs
      .groupBy("df1.passengerId", "df2.Passenger2Id") // Group by the two passenger IDs
      .agg(count("*").as("Number of Flights Together")) // Count the number of shared flights
      .filter(col("Number of Flights Together") > 3) // Filter out pairs with <= 3 shared flights
    //debuging
    groupedPairs.show()
    log.info("groupedPairs:\n" + groupedPairs.show(100, truncate = false))
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
        filteredFlights.show()
        log.info("filteredFlights:\n" + filteredFlights.show(20, truncate = false))

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
    passengerPairs.show()
    log.info("passengerPairs:\n" + passengerPairs.show(20, truncate = false))

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
     // .filter(col("Number of Flights Together") > atLeastNTimes) // Filter pairs flying together more than the threshold
    groupedPairs.show()
    log.info("groupedPairs:\n" + groupedPairs.show(20, truncate = false))
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
//
//    // Question 1
//
//
//
//    // Step 2: Call the function
//    val totalFlights = totalFlightsPerMonth(flightData.collect().toSeq)
//    // debugging
////    println("Final Output:")
////    totalFlights.foreach(flight =>
////      println(s"Month: ${flight.`Month`}, Number of Flights: ${flight.`Number of Flights`}")
////    )
//    // Step 3: Convert to Dataset and write to CSV
//    val totalFlightsDS = totalFlights.toDS()
//    totalFlightsDS.write
//      .mode("overwrite")
//      .option("header", "true")
//      .csv(s"$outputDirectory/Q1_totalFlights")
//
//    println(s"Data successfully written to ${outputDirectory}/Q1_totalFlights")
//
//    // Question 2
//    val frequentFlyers = mostFrequentFlyers(flightData, passengers)
//    //val tableRequentFlyers = frequentFlyers.show(100, truncate = false) // Fetch first 100 rows as a string
//    // log.info("tableRequentFlyers:\n" + tableRequentFlyers) // Log the table
//    frequentFlyers.write.mode("overwrite").option("header", "true").csv(s"$outputDirectory/Q2_frequentFlyers")
//
//    // Question 3
//    val longestRun = longestRunWithoutUK(flightData.collect().toSeq)
//   //  debugging
////        println("Final Output:")
////        longestRun.foreach(flight =>
////          println(s"Passenger ID: ${flight.`Passenger ID`}, Longest Run: ${flight.`Longest Run`}")
////        )
//
//    val longestRunDS = longestRun.toDS()
//    longestRunDS.write
//      .mode("overwrite")
//      .option("header", "true")
//      .csv(s"$outputDirectory/Q3_longestRun")
//
//    println(s"Data successfully written to ${outputDirectory}/Q3_longestRun")
//
//    // Question 4
    val togetherMoreThan3 = flightsTogether(flightData)
    //      val tabletogetherMoreThan3 = togetherMoreThan3.show(10, truncate = false) // Fetch first 10 rows as a string
    //      log.info("tablelongestRunWithoutUK:\n" + tabletogetherMoreThan3) // Log the table
    togetherMoreThan3.write.mode("overwrite").option("header", "true").csv(s"$outputDirectory/Q4_flightsTogether")

    // Extra Task
//    val fromDate = Date.valueOf(LocalDate.parse("2017-01-01"))
//    val toDate = Date.valueOf(LocalDate.parse("2017-12-31"))
//    val togetherInRange = flownTogetherWithinRange(flightData, 3, fromDate, toDate)
//
//    //    val tableTogetherInRange = togetherInRange.show(10, truncate = false) // Fetch first 10 rows as a string
//    //    log.info("tableTogetherInRange:\n" + tableTogetherInRange) // Log the table
//
//    togetherInRange.write.mode("overwrite").option("header", "true").csv(s"$outputDirectory/Extra_flightsTogetherInRange")

    // free up the memory after use
    flightData.unpersist()
    passengers.unpersist()
    log.info("All tasks completed. CSV files saved in the output directory.")
  }
}