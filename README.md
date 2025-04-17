# flight_assignment
# Flight Assignment Analysis

This project performs an advanced analysis of flight data to answer specific queries related to flight statistics, passenger patterns, and relationships among passengers. The application is built using "Apache Spark" and powered by "Scala" for distributed data processing and large-scale computation.

---

flight_assignment/
├── src/
│   ├── main/scala/
│   │   └── Flight_Assignment.scala       # Core logic
│   └── test/scala/
│       └── Flight_Assignment_Test.scala  # Unit tests
├── data/
│   ├── flightData.csv                    # Flight logs
│   └── passengers.csv                    # Passenger details
├── lib/                                  # External dependencies
├── build.sbt                             # SBT configuration
└── README.md                             # Documentation



## **Table of Contents**
1. [Features](#featuresw)
2. [Prerequisites](#prerequisites)
3. [Input Data](#input-data)
4. [How It Works](#how-it-works)
5. [Setup and Run Instructions](#setup-and-run-instructions)
6. [Code Overview and ScalaTest Expected Output](#code-overview and output-examples)


---

## **Features**

This application provides the following functionalities:
1. **Monthly Flight Count**:
   - Calculates the total number of flights for each month.
2. **Frequent Flyers**:
   - Identifies the top 100 passengers who have flown the most.
3. **Longest Run Without Visiting the UK**:
   - Finds the greatest number of countries a passenger has visited consecutively without visiting the UK.
4. **Passenger Pairs Analysis**:
   - Identifies pairs of passengers who have traveled together on more than 3 flights.
5. **Passenger Pairs Within Specific Date Ranges**:
   - Finds passenger pairs who have flown more than a specified number of shared flights within a given date range.

---

## **Prerequisites**

    1. Java 8 or higher
    2. Scala 2.12.x
    3. Apache Spark 2.4.8
    4. SBT (Scala Build Tool) to manage dependencies and build the project
    5. ScalaTest sbt-1.10.11

---

## **Input Data**

The program requires two CSV files with the following schemas:

### 1. **Flight Data**
File Path: `data/flightData.csv`

| Column Name      | Data Type   | Description                                |
|------------------|-------------|--------------------------------------------|
| `passengerId`    | Integer     | Unique identifier for the passenger.       |
| `flightId`       | Integer     | Unique identifier for the flight.          |
| `from`           | String      | Departure location (airport/country code). |
| `to`             | String      | Arrival location (airport/country code).   |
| `date`           | String      | Flight date in `yyyy-MM-dd` format.        |

### 2. **Passenger Data**
File Path: `data/passengers.csv`

| Column Name      | Data Type   | Description                                |
|------------------|-------------|--------------------------------------------|
| `passengerId`    | Integer     | Unique identifier for the passenger.       |
| `firstName`      | String      | First name of the passenger.               |
| `lastName`       | String      | Last name of the passenger.                |

---

## **How It Works**

The application processes the provided flight and passenger data as follows:

1. **Reads Input Data**: 
   - Load and parse the CSV files using user-defined schemas for structured processing.
   - Validate the input dataframe schema with the expected schema

2. **Calculates Monthly Flights**:
   - Uses Spark SQL to group flights by month and count the total for each month.

3. **Finds Frequent Flyers**:
   - Aggregates the total number of flights for each passenger and joins this result with the passenger details to identify the top 100 passengers.

4. **Analyzes Longest Run Without UK**:
   - Detects uninterrupted journeys without visiting the UK, groups runs by passengers, and identifies the maximum run length for each passenger.

5. **Finds Shared Flights Between Passengers**:
   - Identifies pairs of passengers who have flown together more than 3 times or within a specified date range.

---

## **Setup and Run Instructions**

Follow the steps below to set up and run the application:

### 1. **Clone the Repository**
```bash
git clone <repository-url>
cd <repository-folder>
```

### 2. **Prepare Input Data**
- Place the input CSV files (`flightData.csv` and `passengers.csv`) in the `data/` directory.

### 3. **Compile the Code**
- Compile the code using SBT:
```bash
sbt compile
```

### 4. **Run the Application**
- Start the application using:
```bash
sbt run
```

### 5. **View Output**
- The results will be printed in the console. You can also configure the logging to store results in a file.

---



## **Code Overview and ScalaTest Expected Output**

#### 1. `totalFlightsPerMonth`
Calculates the total number of flights for each month. Groups flights by month extracted from the `date` column.

sample testing data
+--------+-----------+----------+-------+------+
|flightId|passengerId|date      |from   |to    |
+--------+-----------+----------+-------+------+
|F1      |P1         |2025-01-01|US     |UK    |
|F2      |P2         |2025-01-15|UK     |France|
|F3      |P3         |2025-02-01|Germany|Spain |
|F4      |P4         |2025-02-15|Spain  |Italy |
+--------+-----------+----------+-------+------+

result
+-----+-----------------+
|Month|Number of Flights|
+-----+-----------------+
|1    |2                |
|2    |2                |
+-----+-----------------+


---

#### 2. `findTopFlyersByFlightCount`
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

#### 3. `longestRunWithoutUK`
Identifies the maximum number of countries visited by passengers without visiting the UK.
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

#### 4. `flightsTogether`
Finds pairs of passengers who have been on more than 3 flights together. Filters out duplicate pairs and same passengers.
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

#### 5. `flownTogether`
Finds passenger pairs who flew together more than `N` times within a specific date range. Also displays the first and last dates they traveled together.

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

## **Logging and Debugging**

- Logging is implemented using **Apache Log4j**.
- Enable log debugging with relevant information like showing intermediate DataFrame output using `log.info()`.

---

Download link
git clone https://github.com/ytchong222/flight_assignment

