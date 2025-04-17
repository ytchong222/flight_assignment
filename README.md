# Flight Assignment

This project performs an advanced analysis of flight data to answer specific queries related to flight statistics.
The application is built using "Apache Spark" and powered by "Scala" for distributed data processing and large-scale computation.

---

/flight_assignment
    ├── src
    │   ├── main
    │   │   ├── scala
    │   │   │   └── org.tc    
	│   │   │          └──Flight_Assignment     // Main code file
    │   ├── test
    │   │   ├── scala
    │   │   │   └── Flight_Assignment_Test.scala // Unit tests
    ├── data
    │   ├── flightData.csv                      // Input flight info logs data
    │   └── passengers.csv                      // Input customer info data
	├── .gitignore                              // .gitignore file
    ├── build.sbt                               // SBT build file
	└── README.md                               //Documentation



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
   - joining the flight data on flightId and date to find pairs of passengers who were on the same flight
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
   
=== sample input data ===
+-------------+----------+----------+-------+------------+
| passengerId | flightId |   from   |   to  |    date    |
+-------------+----------+----------+-------+------------+
|           1 |      101 |       US |    UK | 2025-01-01 |
|           2 |      102 |       UK | France| 2025-01-15 |
|           3 |      103 |   Germany|  Spain| 2025-02-01 |
|           4 |      104 |    Spain |  Italy| 2025-02-15 |
+-------------+----------+----------+-------+------------+

---

=== Result Dataset ===
+-------+-------------------+
| Month | Number of Flights |
+-------+-------------------+
|     1 |                 2 |  // January
|     2 |                 2 |  // February
+-------+-------------------+

=== Expected Dataset ===
+-------+-------------------+
| Month | Number of Flights |
+-------+-------------------+
|     1 |                 2 |  // January
|     2 |                 2 |  // February
+-------+-------------------+

**testing result:Matched**
---

2. `findTopFlyersByFlightCount`
Finds the top 100 passengers who have flown the most number of flights by joining aggregated flight data with passenger details.

=== sample flight data ===
+-------------+----------+------+----+------------+
| passengerId | flightId | from | to |    date    |
+-------------+----------+------+----+------------+
|           1 |      100 |   US | UK | 2025-01-01 |
|           2 |      200 |   CA | UK | 2025-01-02 |
|           1 |      300 |   FR | UK | 2025-01-03 |
|           3 |      400 |   IT | UK | 2025-01-04 |
|           1 |      500 |   IT | UK | 2025-01-05 |
|           2 |      500 |   US | UK | 2025-01-06 |
+-------------+----------+------+----+------------+

=== sample passenger data ===
+-------------+-----------+----------+
| passengerId | firstName | lastName |
+-------------+-----------+----------+
|           1 |      Jack |      Lee |
|           2 |      Kent |      Tan |
|           3 |    Andrew |     Wong |
|           4 |     Larry |     Chew |
+-------------+-----------+----------+

---

=== Result Dataset ===
+--------------+-------------------+------------+-----------+
| Passenger ID | Number of Flights | First name | Last name |
+--------------+-------------------+------------+-----------+
|            1 |                 3 |       Jack |       Lee |
|            2 |                 2 |       Kent |       Tan |
|            3 |                 1 |     Andrew |      Wong |
+--------------+-------------------+------------+-----------+

=== Expected Dataset ===
+--------------+-------------------+------------+-----------+
| Passenger ID | Number of Flights | First name | Last name |
+--------------+-------------------+------------+-----------+
|            1 |                 3 |       Jack |       Lee |
|            2 |                 2 |       Kent |       Tan |
|            3 |                 1 |     Andrew |      Wong |
+--------------+-------------------+------------+-----------+

**testing result:Matched**

---

 3. `longestRunWithoutUK`
   **Finds the 100 most frequent flyers**:
   - Aggregates the total number of flights for each passenger and joins this result with the passenger details to identify the top 100 passengers.
   

=== sample flight data ===
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

===sample passenger data ===
+-----------+--------+----------+---+----+
|passengerId|flightId|from      |to |date|
+-----------+--------+----------+---+----+
|1          |100     |2023-01-01|US |MY  |
|1          |101     |2023-01-02|JP |TH  |
|1          |102     |2023-01-03|JP |UK  |
|1          |103     |2023-01-04|JP |MY  |
|1          |104     |2023-01-05|ID |TH  |
|2          |105     |2023-01-01|SG |US  |
|2          |106     |2023-01-02|HK |UK  |
|2          |107     |2023-01-03|FR |TH  |
|2          |108     |2023-01-04|IT |MY  |
+-----------+--------+----------+---+----+
---

=== Result Dataset ===
+-------------+-------------+
| passengerId | Longest run |
+-------------+-------------+
|           1 |           2 |  
|           2 |           2 |
+-------------+-------------+

=== Expected Dataset ===
+-------------+-------------+
| passengerId | Longest run |
+-------------+-------------+
|           1 |           2 |  
|           2 |           2 |
+-------------+-------------+

**testing result:Matched**
---

 4. `flightsTogether`
   **Finds pairs of passengers who have been on more than 3 flights together**:
   - joining the flight data on flightId to find pairs of passengers who were on the same flight
   - counting how many times each pair traveled together and filtering the results to only include pairs with a count greater than 3.
   
===sample flight data===
+-------------+----------+------+----+------------+
| passengerId | flightId | from | to |    date    |
+-------------+----------+------+----+------------+
|           1 |      100 |   HK | JP | 2025-01-01 |
|           2 |      100 |   HK | JP | 2025-01-01 |
|           1 |      200 |   UK | US | 2025-01-02 |
|           2 |      200 |   UK | US | 2025-01-02 |
|           1 |      300 |   CA | FR | 2025-01-03 |
|           2 |      300 |   CA | FR | 2025-01-03 |
|           1 |      400 |   FR | US | 2025-01-04 |
|           2 |      400 |   FR | US | 2025-01-04 |
|           3 |      500 |   JP | CN | 2025-02-01 |
|           4 |      500 |   JP | CN | 2025-02-01 |
|           3 |      600 |   IN | AU | 2025-02-02 |
|           4 |      600 |   IN | AU | 2025-02-02 |
+-------------+----------+------+----+------------+


---


 
=== Result Dataset ===
+-------------+-------------+----------------------------+
| Passenger 1 | Passenger 2 | Number of flights together |
+-------------+-------------+----------------------------+
|           1 |           2 |                          4 |
+-------------+-------------+----------------------------+


=== Expected Dataset ===
+-------------+-------------+----------------------------+
| Passenger 1 | Passenger 2 | Number of flights together |
+-------------+-------------+----------------------------+
|           1 |           2 |                          4 |
+-------------+-------------+----------------------------+

**testing result:Matched**
---

 5. `flownTogether`
  **Finds pairs of passengers who have been on more than N flights together within a specific date range**:
   - filtering flights within the given date range, 
   - joining the data on flightId to find passenger pairs, 
   - counting how many times each pair traveled together and selecting only the pairs with a count greater than N

sample flight data
+-------------+----------+------+----+------------+
| passengerId | flightId | from | to |    date    |
+-------------+----------+------+----+------------+
|           1 |        1 |   HK | ID | 2025-01-01 |
|           2 |        1 |   HK | ID | 2025-01-01 |
|           1 |        2 |   TW | ID | 2025-01-02 |
|           2 |        2 |   TW | ID | 2025-01-02 |
|           1 |        3 |   TH | SG | 2025-01-03 |
|           2 |        3 |   TH | SG | 2025-01-03 |
|           1 |        4 |   TH | US | 2025-01-04 |
|           2 |        4 |   TH | US | 2025-01-04 |
|           3 |        5 |   HK | SG | 2025-01-05 |
|           4 |        6 |   HK | US | 2025-01-06 |
+-------------+----------+------+----+------------+

---
=== Result Dataset ===
+-----------------+-----------------+----------------------------+------------+------------+
| Passenger 1 ID  | Passenger 2 ID  | Number of flights together |    From    |     To     |
+-----------------+-----------------+----------------------------+------------+------------+
|               1 |               2 |                          4 | 2025-01-01 | 2025-01-04 |
+-----------------+-----------------+----------------------------+------------+------------+

=== Expected Dataset ===
+-----------------+-----------------+----------------------------+------------+------------+
| Passenger 1 ID  | Passenger 2 ID  | Number of flights together |    From    |     To     |
+-----------------+-----------------+----------------------------+------------+------------+
|               1 |               2 |                          4 | 2025-01-01 | 2025-01-04 |
+-----------------+-----------------+----------------------------+------------+------------+



**testing result:Matched**
---

## Logging and Debugging

- Logging is enabled via **Apache Log4j**.
- Use `log.info()` for inspecting outputs and debugging key steps.

---



