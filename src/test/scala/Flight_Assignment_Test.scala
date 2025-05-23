package org.tc

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Date



class Flight_Assignment_Test extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder()
    .appName("Flight Assigment Test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Use the same case class as `FlightData` defined in your main application




  test("totalFlightsPerMonth should calculate the total number of flights per month") {
    println("=== start DataSet ===")
    val sampleFlights: Seq[FlightData] = Seq(
      FlightData(1, 101, "US", "UK", "2025-01-01"),
      FlightData(6, 101, "US", "UK", "2025-01-01"),
      FlightData(2, 102, "UK", "France", "2025-01-15"),
      FlightData(3, 103, "Germany", "Spain", "2025-02-01"),
      FlightData(4, 104, "Spain", "Italy", "2025-02-15")
    )
    val result: Seq[MonthlyFlights] = Flight_Assignment.totalFlightsPerMonth(sampleFlights)

   // val result: Dataset[MonthlyFlights] = Flight_Assignment.totalFlightsPerMonth(sampleFlights)
    println("=== start2 DataSet ===")
    val expected: Seq[MonthlyFlights] = Seq(
      MonthlyFlights(1, 2), // January
      MonthlyFlights(2, 2)  // February
    )
    // Debug Result
//    println("=== Result Data ===")
//    result.foreach(flight =>
//      println(s"Month: ${flight.`Month`}, Number of Flights: ${flight.`Number of Flights`}")
//    )

    assert(result.toSet == expected.toSet)
    println("=== End Test ===")
  }

//
  // Test for `findTopFlyersByFlightCount`
  test("findTopFlyersByFlightCount based on flight count") {
    // Create a DataFrame for flight data
    val flightData = Seq(
      FlightData(1, 100, "US", "UK", "2025-01-01"),
      FlightData(2, 200, "CA", "UK", "2025-01-02"),
      FlightData(1, 300, "FR", "UK", "2025-01-03"),
      FlightData(3, 400, "IT", "UK", "2025-01-04"),
      FlightData(1, 500, "IT", "UK", "2025-01-05"),
      FlightData(2, 500, "US", "UK", "2025-01-06")
    ).toDS()

    // Create a DataFrame for passenger data
    val passengers = Seq(
      Passenger(1, "Jack", "Lee"),
      Passenger(2, "Kent", "Tan"),
      Passenger(3, "Andrew", "Wong"),
      Passenger(4, "Larry", "Chew")
    ).toDS()

    // Expected output as a DataFrame
    val expectedOutput = Seq(
      FrequentFlyer(1, 3, "Jack", "Lee"),
      FrequentFlyer(2, 2, "Kent", "Tan"),
      FrequentFlyer(3, 1, "Andrew", "Wong")
    ).toDS()

    val result: Dataset[FrequentFlyer] = Flight_Assignment.mostFrequentFlyers(flightData, passengers)

    // Collect results and expected output as sets for comparison
    val resultSet = result.collect().toSet
    val expectedSet = expectedOutput.collect().toSet
    println("=== result DataSet ===")
    result.show(truncate = false)
    // Assert that the resulting DataFrame matches the expected output
    assert(resultSet == expectedSet)
  }

  test("longestRunWithoutUK should calculate the correct longest run for each passenger") {
    // Define test flight data
    val flightData: Seq[FlightData] = Seq(

//      FlightData(4943,112,	"us",	"fr",	"2/9/2017"),
//      FlightData(4943,126,	"fr",	"ir",	"2/9/2017")

      FlightData(1, 100, "UK", "FR", "1/1/2023"),  // Passenger 1, country UK (resets 0), FR 1
      FlightData(1, 101, "US", "CN", "1/2/2023"),  // Passenger 1, country US  2,CN 3
      FlightData(1, 102, "UK", "DE", "1/3/2023"),  // Passenger 1, country UK (resets 0),DE 1
      FlightData(1, 103, "UK", "CN", "1/4/2023"),  // Passenger 1, country  UK (resets 0),CN 1
      FlightData(2, 105, "SG", "US", "1/1/2023"), // Passenger 2,  country US  1,CN 2
      FlightData(2, 106, "HK", "UK", "1/2/2023"),  // Passenger 2, country to HK 3, UK   (resets 0)
      FlightData(2, 107, "FR", "TH", "1/3/2023"),  // Passenger 2, country to FR   1, TH 2
      FlightData(2, 108, "IT", "MY", "1/4/2023")   // Passenger 2, country to IT   3, MY 4
    )

    // Define expected output
    val expectedOutput: Seq[LongestRunResult] = Seq(
      LongestRunResult(2, 4),  // P2's longest run without UK  ("FR", "TH","IT","MY") 4
      LongestRunResult(1, 3) // P1's longest run without UK  (FR,US,CN) 3

    )

    // Call the function under test
    val result: Seq[LongestRunResult] = Flight_Assignment.longestRunWithoutUK(flightData)


    // Debugging
      println("=== Result Data ===")
      result.foreach(flight =>
        println(s"Passenger: ${flight.`Passenger ID`}, Number of Flights: ${flight.`Longest Run`}")
      )

    assert(result.toSet == expectedOutput.toSet)
//    println("=== End Test ===")
  }

  test("flightsTogether should correctly identify pairs of passengers with more than 3 shared flights") {
    // Define the test input `flightData`
    val flightData = Seq(
      // Passenger 1 and 2 share 4 flights on the same date
      FlightData(1, 100, "HK", "JP", "2025-01-01"), // Shared flight
      FlightData(2, 100, "HK", "JP", "2025-01-01"), // Shared flight
      FlightData(1, 200, "UK", "US", "2025-01-02"), // Shared flight
      FlightData(2, 200, "UK", "US", "2025-01-02"), // Shared flight
      FlightData(1, 300, "CA", "FR", "2025-01-03"), // Shared flight
      FlightData(2, 300, "CA", "FR", "2025-01-03"), // Shared flight
      FlightData(1, 400, "FR", "US", "2025-01-04"), // Shared flight
      FlightData(2, 400, "FR", "US", "2025-01-04"), // Shared flight

      // Passenger 3 and 4 share 2 flights on the same date less than 3
      FlightData(3, 500, "JP", "CN", "2025-02-01"), // Shared flight
      FlightData(4, 500, "JP", "CN", "2025-02-01"), // Shared flight
      FlightData(3, 600, "IN", "AU", "2025-02-02"), // Shared flight
      FlightData(4, 600, "IN", "AU", "2025-02-02")  // Shared flight
    ).toDS()

    val expectedOutput = Seq(
      FlightsTogetherResult(1, 2, 4)
    ).toDS



    val result: Dataset[FlightsTogetherResult]  = Flight_Assignment.flightsTogether(flightData)
    // Debugging outputs to verify schemas and values
//      println("=== source DataFrame ===")
//      flightData.show(false)
       println("=== Result DataFrame ===")
        result.show(false)
    //   result.printSchema()
        println("=== Expected DataFrame ===")
        expectedOutput.show(false)
    //    expectedOutput.printSchema()

    // Validate the result
    assert(result.collect().toSet == expectedOutput.collect().toSet)
  }


  test("flownTogether should find passengers who have flown together more than N times within a date range") {
    import spark.implicits._

    // Define input data for flightData as Dataset[FlightData]
    val flightData: Dataset[FlightData] = Seq(
      FlightData(1, 1, "HK", "ID", "2025-01-01"),
      FlightData(2, 1, "HK", "ID", "2025-01-01"),
      FlightData(1, 2, "TW", "ID", "2025-01-02"),
      FlightData(2, 2, "TW", "ID", "2025-01-02"),
      FlightData(1, 3, "TH", "SG", "2025-01-03"),
      FlightData(2, 3, "TH", "SG", "2025-01-03"),
      FlightData(1, 4, "TH", "US", "2025-01-04"),
      FlightData(2, 4, "TH", "US", "2025-01-04"),
      FlightData(3, 5, "HK", "SG", "2025-01-05"),
      FlightData(4, 6, "HK", "US", "2025-01-06")
    ).toDS()

    // Define the constraints
    val atLeastNTimes = 3
    val dateFrom = Date.valueOf("2025-01-01") // Start date
    val dateTo = Date.valueOf("2025-01-05")   // End date

    // Define the expected output as Dataset[FlightsTogetherResult]
    val expectedOutput: Dataset[FlightsTogetherWithRange] = Seq(
      FlightsTogetherWithRange(1, 2, 4, Date.valueOf("2025-01-01"), Date.valueOf("2025-01-04"))
    ).toDS()

    // Invoke the function under test (replace Flight_Assignment.flownTogether with your implementation)
    val result: Dataset[FlightsTogetherWithRange] = Flight_Assignment.flownTogetherWithinRange(flightData, atLeastNTimes, dateFrom, dateTo)

    // Validate the result
    assert(result.collect().toSet == expectedOutput.collect().toSet)
  }
}