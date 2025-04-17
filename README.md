# Flight Assignment Analysis

This project performs an advanced analysis of flight data to answer specific queries related to flight statistics.
The application is built using "Apache Spark" and powered by "Scala" for distributed data processing and large-scale computation.

---

root directory
    ├── src
    │   ├── main
    │   │   ├── scala
    │   │   │   └── Flight_Assignment.scala    // Main code file
    │   ├── test
    │   │   ├── scala
    │   │   │   └── Flight_Assignment_Test.scala // Unit tests
    ├── data
    │   ├── flightData.csv                      // Input flight info logs data
    │   ├── passengers.csv                      // Input customer info data
    ├── lib                                    // Dependencies, e.g., Spark
    ├── build.sbt                              // SBT build file
	└── README.md                              //Documentation



## **Table of Contents**
1. [Questions Addressed]
2. [Setup Instructions]
3. [Functional Details]
4. [How It Works]
5. [Setup and Run Instructions]
6. [Code Overview and ScalaTest Expected Output]
7. [Logging and Debugging]

---

### Questions Addressed:

1. Find total number of flights per month.
2. Find 100 most frequent flyers (frequent passengers).
3. Find longest number of countries a passenger visited without entering the UK in a sequence.
4. Find passengers who have been on more than 3 flights together or `N`+ flights together within a date range.
5. Find passenger Pairs Within Specific Date Ranges

## Setup Instructions
To run this project, you’ll need the following software:

- **Java 8** or later
- **Scala 2.12.x**
- **Apache Spark 2.4.8**
- **SBT (Scala Build Tool)**
- **ScalaTest** (`"org.scalatest" %% "scalatest" % "3.2.17" % Test`)


---

### Getting Started

1. Clone the repository:

   ```bash
   git clone https://github.com/ytchong222/flight_assignment.git
   cd flight_assignment
   ```

2. Place the required input files (`flightData.csv` and `passengers.csv`) in the `data` directory.

3. Build the project:

   ```bash
   sbt clean compile
   ```

4. Run the application:

   ```bash
   sbt run
   ```

5. Execute unit tests:

   ```bash
   sbt testOnly Flight_Assignment_Test
   ```

   Logs and output will be displayed on the console.

---


## Functional Details:


### Main Workflow

The application consists of the following key components:

1. **Input Parsing**:
   - Reads `flightData.csv` and `passengers.csv` with Spark and validates schemas.

2. **Spark SQL**:
   - Processes large datasets efficiently for tasks like grouping, filtering, and aggregation.

3. **Functional Pipelines**:
   - Ensures modular, reusable computations.


### Input Data Requirements

**1. Flight Data**  
Path: `data/flightData.csv`

| Column Name      | Data Type   | Description                                |
|------------------|-------------|--------------------------------------------|
| `passengerId`    | Integer     | Unique identifier for the passenger.       |
| `flightId`       | Integer     | Unique identifier for the flight.          |
| `from`           | String      | Departure location (e.g., country code).   |
| `to`             | String      | Arrival location (e.g., country code).     |
| `date`           | String      | Flight date in `yyyy-MM-dd` format.        |

**2. Passenger Data**  
Path: `data/passengers.csv`

| Column Name      | Data Type   | Description                                |
|------------------|-------------|--------------------------------------------|
| `passengerId`    | Integer     | Unique identifier for the passenger.       |
| `firstName`      | String      | First name of the passenger.               |
| `lastName`       | String      | Last name of the passenger.                |


---

## How It Works

The application processes the provided flight and passenger data as follows:

1. **Reads Input Data**: 
   - Load and parse the CSV files using user-defined schemas for structured processing.
   - Validate the input dataframe schema with the expected schema

2. **Calculates Monthly Flights**:
   - group flights by month and count the total for each month.

3. **Finds the 100 most frequent flyers**:
   - Aggregates the total number of flights for each passenger and joins this result with the passenger details to identify the top 100 passengers.

4. **Finds the greatest number of countries a passenger has visited without visiting the UK**:
   - Groups consecutive rows of flight data per passenger using window functions, filters out rows with UK as the destination.
   - calculates the maximum run length of uninterrupted visits to non-UK countries.

5. **Finds pairs of passengers who have been on more than 3 flights together**:
   - joining the flight data on flightId to find pairs of passengers who were on the same flight
   - counting how many times each pair traveled together and filtering the results to only include pairs with a count greater than 3.
   
6. **Finds pairs of passengers who have been on more than N flights together within a specific date range**:
   - filtering flights within the given date range, 
   - joining the data on flightId to find passenger pairs, 
   - counting how many times each pair traveled together and selecting only the pairs with a count greater than N


---




## Code Overview and ScalaTest Expected Output

 1. `totalFlightsPerMonth`
   **Calculates Monthly Flights**:
   - group flights by month and count the total for each month.
   
sample testing data
+--------+-----------+----------+-------+------+
|flightId|passengerId|date      |from   |to    |
+--------+-----------+----------+-------+------+
|F1      |P1         |2025-01-01|US     |UK    |
|F2      |P2         |2025-01-15|UK     |France|
|F3      |P3         |2025-02-01|Germany|Spain |
|F4      |P4         |2025-02-15|Spain  |Italy |
+--------+-----------+----------+-------+------+

result output
+-----+-----------------+
|Month|Number of Flights|
+-----+-----------------+
|1    |2                |
|2    |2                |
+-----+-----------------+


---

2. `findTopFlyersByFlightCount`
Finds the top 100 passengers who have flown the most number of flights by joining aggregated flight data with passenger details.

sample flight data
+--------+-----------+
|flightId|passengerId|
+--------+-----------+
|F1      |P1         |
|F2      |P2         |
|F3      |P1         |
|F4      |P3         |
|F5      |P1         |
|F6      |P2         |
+--------+-----------+

sample passenger data
+-----------+---------+--------+
|passengerId|firstName|lastName|
+-----------+---------+--------+
|P1         |Jack     |Lee     |
|P2         |Kent     |Tan     |
|P3         |Andrew   |Wong    |
|P4         |Larry    |Chew    |
+-----------+---------+--------+

---

result output
+-----------+-----------------+---------+--------+
|Passenger Id|Number of Flights|First name|Last name|
+-----------+-------------------+---------+--------+
|P1         |3                 |Jack      |Lee     |
|P2         |2                 |Kent      |Tan     |
|P3         |1                 |Andrew    |Wong    |
+-----------+------------------+----------+--------+


---

 3. `longestRunWithoutUK`
   **Finds the 100 most frequent flyers**:
   - Aggregates the total number of flights for each passenger and joins this result with the passenger details to identify the top 100 passengers.
   

sample flight data
+-----------+----------+---+
|passengerId|date      |to |
+-----------+----------+---+
|P1         |2025-01-01|MY |
|P1         |2025-01-02|TH |
|P1         |2025-01-03|UK |
|P1         |2025-01-04|ID |
|P1         |2025-01-05|SG |
|P1         |2025-01-06|UK |
|P2         |2025-01-01|US |
|P2         |2025-01-02|TH |
|P2         |2025-01-03|UK |
|P2         |2025-01-04|MY |
+-----------+----------+---+

---

result output
+------------+-----------+
|Passenger Id|Longest Run|
+------------+-----------+
|P1          |2          |
|P2          |2          |
+------------+-----------+

---

 4. `flightsTogether`
   **Finds pairs of passengers who have been on more than 3 flights together**:
   - joining the flight data on flightId to find pairs of passengers who were on the same flight
   - counting how many times each pair traveled together and filtering the results to only include pairs with a count greater than 3.
   
sample flight data
+--------+-----------+
|flightId|passengerId|
+--------+-----------+
|F1      |P1         |
|F1      |P2         |
|F1      |P3         |
|F2      |P1         |
|F2      |P2         |
|F3      |P1         |
|F3      |P2         |
|F3      |P3         |
|F4      |P1         |
|F4      |P2         |
+--------+-----------+

---


result output
+--------------+--------------+--------------------------+
|Passenger 1 ID|Passenger 2 ID|Number of Flights Together|
+--------------+--------------+--------------------------+
|P1            |P2            |4                         |
+--------------+--------------+--------------------------+

---

 5. `flownTogether`
  **Finds pairs of passengers who have been on more than N flights together within a specific date range**:
   - filtering flights within the given date range, 
   - joining the data on flightId to find passenger pairs, 
   - counting how many times each pair traveled together and selecting only the pairs with a count greater than N

sample flight data
+--------+-----------+----------+----+---+
|flightId|passengerId|date      |from|to |
+--------+-----------+----------+----+---+
|F1      |P1         |2025-01-01|HK  |ID |
|F1      |P2         |2025-01-01|HK  |ID |
|F2      |P1         |2025-01-02|TW  |ID |
|F2      |P2         |2025-01-02|TW  |ID |
|F3      |P1         |2025-01-03|TH  |SG |
|F3      |P2         |2025-01-03|TH  |SG |
|F4      |P1         |2025-01-04|TH  |US |
|F4      |P2         |2025-01-04|TH  |US |
|F5      |P3         |2025-01-05|HK  |SG |
|F6      |P4         |2025-01-06|HK  |US |
+--------+-----------+----------+----+---+

---

result output
+--------------+--------------+--------------------------+----------+----------+
|Passenger 1 ID|Passenger 2 ID|Number of Flights Together|From      |To        |
+--------------+--------------+--------------------------+----------+----------+
|P1            |P2            |4                         |2025-01-01|2025-01-04|
+--------------+--------------+--------------------------+----------+----------+


---

## Logging and Debugging

- Logging is enabled via **Apache Log4j**.
- Use `log.info()` for inspecting outputs and debugging key steps.

---


