
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite// ScalaTest unit tests based on the FunSuite style
import org.apache.spark.sql.functions._

import java.sql.Date

// Import the Flight_Assignment object
import Flight_Assignment._

class Flight_Assignment_Test extends AnyFunSuite {

  // Initialize SparkSession for testing
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("Flight_Assignment_Test")
    .master("local[*]") // Use local Spark mode for testing
    .getOrCreate()

  import spark.implicits._

  // Sample flight DataFrame used for testing
  val sampleFlights: DataFrame = Seq(
    ("F1", "P1", "2025-01-01", "US", "UK"),
    ("F2", "P2", "2025-01-15", "UK", "France"),
    ("F3", "P3", "2025-02-01", "Germany", "Spain"),
    ("F4", "P4", "2025-02-15", "Spain", "Italy")
  ).toDF("flightId", "passengerId", "date", "from", "to")

  test("totalFlightsPerMonth") {
    // Run the method under test
    val result = totalFlightsPerMonth(sampleFlights)
    // Debugging outputs to verify schemas and values
    // println("=== Source DataFrame ===")
    // sampleFlights.show(truncate = false)
    // println("=== Result DataFrame ===")
    // result.show(false)
    // result.printSchema()

    // Collect the DataFrame into an array of Rows
    val collectedResult = result.collect()

    // Assert results (January: 2 flights, February: 2 flights)
    assert(collectedResult.contains(Row(1, 2))) // January (Month = 1) has 2 flights
    assert(collectedResult.contains(Row(2, 2))) // February (Month = 2) has 2 flights
  }

  test("findTopFlyersByFlightCount based on flight count") {
    // Create a DataFrame for flightData
    val flightData = Seq(
      ("F1", "P1"),
      ("F2", "P2"),
      ("F3", "P1"),
      ("F4", "P3"),
      ("F5", "P1"),
      ("F6", "P2"),
    ).toDF("flightId", "passengerId")

    // Create a DataFrame for passengers
    val passengers = Seq(
      ("P1", "Jack","Lee"),
      ("P2", "Kent","Tan"),
      ("P3", "Andrew","Wong"),
      ("P4", "Larry","Chew")
    ).toDF("passengerId", "firstName", "lastName")

    // Expected output as a DataFrame
    val expectedOutput = Seq(
      ("P1", 3, "Jack","Lee"),
      ("P2", 2, "Kent","Tan"),
      ("P3", 1, "Andrew","Wong")
    ).toDF("passengerId", "Number of Flights", "firstName", "lastName").orderBy(desc("Number of Flights"))

    val result: DataFrame = Flight_Assignment.findTopFlyersByFlightCount(flightData, passengers)
    // Debugging outputs to verify schemas and values
//    println("=== flightData DataFrame ===")
//    flightData.show(false)
//
//    println("=== passengers DataFrame ===")
//    passengers.show(false)
//
//    println("=== Result DataFrame ===")
//    result.show(false)        // Call show() directly on the result DataFrame
//    result.printSchema()      // Call printSchema() directly on the result DataFrame
//    println("=== Expected DataFrame ===")
//    expectedOutput.show(false)
//    expectedOutput.printSchema()
    // Collect results and expected output as sets for comparison
    val resultSet = result.collect().toSet
    val expectedSet = expectedOutput.collect().toSet

    // Assert that the resulting DataFrame matches the expected output
    assert(resultSet == expectedSet)
  }

  test("longestRunWithoutUK should calculate the longest run") {
    // Define test `flightData`
    val flightData = Seq(
      ("P1", "2025-01-01", "MY"),  // Passenger 1, date, flight to MALAYSIA
      ("P1", "2025-01-02", "TH"),  // Passenger 1, date, flight to Thailand
      ("P1", "2025-01-03", "UK"),  // Passenger 1, date, flight to UK (run resets here)
      ("P1", "2025-01-04", "ID"),  // Passenger 1, date, flight to Indonesia
      ("P1", "2025-01-05", "SG"),  // Passenger 1, date, flight to SG
      ("P1", "2025-01-06", "UK"),  // Passenger 1, date, flight to UK (run resets here)
      ("P2", "2025-01-01", "US"),  // Passenger 2, date, flight to the US
      ("P2", "2025-01-02", "TH"),  // Passenger 2, date, flight to TH
      ("P2", "2025-01-03", "UK"),  // Passenger 2, date, flight to UK    (run resets here)
      ("P2", "2025-01-04", "MY")   // Passenger 2, date, flight to MALAYSIA
    ).toDF("passengerId", "date", "to")

    // Longest run of avoiding "UK" for each passenger
    val expectedOutput = Seq(
      ("P1", 2), // P1's longest run without UK is 2
      ("P2", 2)  // P2's longest run without UK is 2
    ).toDF("passengerId", "Longest Run")

    // Call the function under test
    val result: DataFrame = Flight_Assignment.longestRunWithoutUK(flightData)
    // Debugging outputs to verify schemas and values
        //println("=== source DataFrame ===")
        //flightData.show(false)
        println("=== Result DataFrame ===")
        result.show(false)
    //    result.printSchema()
    //    println("=== Expected DataFrame ===")
    //    expectedOutput.show(false)
    //    expectedOutput.printSchema()
    // Validate the result
    assert(result.collect().toSet == expectedOutput.collect().toSet)
  }

  test("flightsTogether should correctly identify pairs of passengers with more than 3 shared flights") {
    // Define the test input `flightData`
    val flightData = Seq(
      ("F1", "P1"),
      ("F1", "P2"),
      ("F1", "P3"),
      ("F2", "P1"),
      ("F2", "P2"),
      ("F3", "P1"),
      ("F3", "P2"),
      ("F3", "P3"),
      ("F4", "P1"),
      ("F4", "P2")
    ).toDF("flightId", "passengerId")

    val expectedOutput = Seq(
      ("P1", "P2", 4)
    ).toDF("passengerId", "Passenger2Id", "Number of Flights Together")

    // Invoke the function under test
    val result: DataFrame = Flight_Assignment.flightsTogether(flightData)
    // Debugging outputs to verify schemas and values
    //  println("=== source DataFrame ===")
    //  flightData.show(false)
    //  println("=== Result DataFrame ===")
    //   result.show(false)
    //   result.printSchema()
    //    println("=== Expected DataFrame ===")
    //    expectedOutput.show(false)
    //    expectedOutput.printSchema()

    // Validate the result
    assert(result.collect().toSet == expectedOutput.collect().toSet)
  }

  test("flownTogether should find passengers who have flown together more than N times within a date range") {
    // Define test input data for flightData
    val flightData = Seq(
      ("F1", "P1", Date.valueOf("2025-01-01"), "HK", "ID"),
      ("F1", "P2", Date.valueOf("2025-01-01"), "HK", "ID"),
      ("F2", "P1", Date.valueOf("2025-01-02"), "TW", "ID"),
      ("F2", "P2", Date.valueOf("2025-01-02"), "TW", "ID"),
      ("F3", "P1", Date.valueOf("2025-01-03"), "TH", "SG"),
      ("F3", "P2", Date.valueOf("2025-01-03"), "TH", "SG"),
      ("F4", "P1", Date.valueOf("2025-01-04"), "TH", "US"),
      ("F4", "P2", Date.valueOf("2025-01-04"), "TH", "US"),
      ("F5", "P3", Date.valueOf("2025-01-05"), "HK", "SG"),
      ("F6", "P4", Date.valueOf("2025-01-06"), "HK", "US")
    ).toDF("flightId", "passengerId", "date", "from", "to")

    val atLeastNTimes = 3
    val dateFrom = Date.valueOf("2025-01-01") // Start date
    val dateTo = Date.valueOf("2025-01-05")   // End date

    val expectedOutput = Seq(
      ("P1", "P2", 4, "2025-01-01", "2025-01-04")
    ).toDF("Passenger 1 ID", "Passenger 2 ID", "Number of Flights Together", "From", "To")

    // Invoke the function under test
    val result = Flight_Assignment.flownTogether(flightData, atLeastNTimes, dateFrom, dateTo)
    //     flightData.show(false)
    //     println("=== result DataFrame ===")
    //     result.show(false)

    // Validate the result
    assert(result.collect().toSet == expectedOutput.collect().toSet)
    }
}


