# Purpose
pure scala version of https://github.com/jgperrin/net.jgp.books.spark.ch12

# Environment
- Java 8
- Scala 2.13.8
- Spark 3.2.1

# How to run
## 1, sbt package, in project root dir
When success, there a jar file at ./target/scala-2.13. The name is `main-scala-ch12_2.13-1.0.jar` (the same as name property in sbt file)

## 2, submit jar file, in project root dir
```
// common jar, need --jars option
$ YOUR_SPARK_HOME/bin/spark-submit \
  --class net.jgp.books.spark.MainApp \
  --master "spark://10.11.1.235:7077" \
  --jars jars/scala-logging_2.13-3.9.4.jar \
  target/scala-2.13/main-scala-ch12_2.13-1.0.jar
```

## 3, print

### Case: RecordTransformation
```

root
 |-- id: integer (nullable = true)
 |-- real2010: integer (nullable = true)
 |-- est2010: integer (nullable = true)
 |-- est2011: integer (nullable = true)
 |-- est2012: integer (nullable = true)
 |-- est2013: integer (nullable = true)
 |-- est2014: integer (nullable = true)
 |-- est2015: integer (nullable = true)
 |-- est2016: integer (nullable = true)
 |-- est2017: integer (nullable = true)
 |-- state: string (nullable = true)
 |-- county: string (nullable = true)
 |-- stateId: integer (nullable = true)
 |-- countyId: integer (nullable = true)

+-----+--------+-------+-------+-------+-------+-------+-------+-------+-------+----------+----------------+-------+--------+
|id   |real2010|est2010|est2011|est2012|est2013|est2014|est2015|est2016|est2017|state     |county          |stateId|countyId|
+-----+--------+-------+-------+-------+-------+-------+-------+-------+-------+----------+----------------+-------+--------+
|4027 |195751  |197124 |202581 |202105 |201810 |203039 |203558 |205463 |207534 |Arizona   |Yuma County     |4      |27      |
|5057 |22609   |22599  |22490  |22349  |22430  |22348  |22103  |22026  |21861  |Arkansas  |Hempstead County|5      |57      |
|6051 |14202   |14204  |14329  |14248  |13997  |14072  |13954  |14101  |14168  |California|Mono County     |6      |51      |
|12035|95696   |96071  |97485  |98506  |99901  |102118 |104739 |107805 |110510 |Florida   |Flagler County  |12     |35      |
|13065|6798    |6771   |6733   |6709   |6775   |6800   |6850   |6789   |6727   |Georgia   |Clinch County   |13     |65      |
+-----+--------+-------+-------+-------+-------+-------+-------+-------+-------+----------+----------------+-------+--------+
only showing top 5 rows

root
 |-- real2010: integer (nullable = true)
 |-- est2010: integer (nullable = true)
 |-- est2017: integer (nullable = true)
 |-- state: string (nullable = true)
 |-- county: string (nullable = true)
 |-- stateId: integer (nullable = true)
 |-- countyId: integer (nullable = true)
 |-- diff: integer (nullable = true)
 |-- growth: integer (nullable = true)

+--------+-------+--------+----------+------------------+-------+--------+-----+------+
|real2010|est2010|est2017 |state     |county            |stateId|countyId|diff |growth|
+--------+-------+--------+----------+------------------+-------+--------+-----+------+
|4092459 |4107854|4652980 |Texas     |Harris County     |48     |201     |15395|545126|
|3817117 |3824644|4307033 |Arizona   |Maricopa County   |4      |13      |7527 |482389|
|9818605 |9824490|10163507|California|Los Angeles County|6      |37      |5885 |339017|
|1931249 |1937378|2188649 |Washington|King County       |53     |33      |6129 |251271|
|1951269 |1952906|2204079 |Nevada    |Clark County      |32     |3       |1637 |251173|
+--------+-------+--------+----------+------------------+-------+--------+-----+------+
only showing top 5 rows
```

### Case: Join
```
root
 |-- countyId: integer (nullable = true)
 |-- county: string (nullable = true)
 |-- pop2017: integer (nullable = true)

+--------+------------------------+-------+
|countyId|county                  |pop2017|
+--------+------------------------+-------+
|1013    |Butler County, Alabama  |19825  |
|1021    |Chilton County, Alabama |44067  |
|1033    |Colbert County, Alabama |54500  |
|1055    |Etowah County, Alabama  |102755 |
|1059    |Franklin County, Alabama|31495  |
+--------+------------------------+-------+
only showing top 5 rows


root
 |-- location: string (nullable = true)
 |-- zip: string (nullable = true)

+-----------------------------------+-----+
|location                           |zip  |
+-----------------------------------+-----+
|Alabama A & M University           |35762|
|University of Alabama at Huntsville|35899|
|Athens State University            |35611|
|Auburn University-Montgomery       |36117|
|Concordia College Alabama          |36701|
+-----------------------------------+-----+
only showing top 5 rows

root
 |-- county: integer (nullable = true)
 |-- zip: integer (nullable = true)

+------+-----+
|county|zip  |
+------+-----+
|1001  |36749|
|1001  |36066|
|1003  |36507|
|1003  |36535|
|1003  |36530|
+------+-----+
only showing top 5 rows

root
 |-- location: string (nullable = true)
 |-- county: integer (nullable = true)
 |-- zip: integer (nullable = true)

+-----------------------------------------+------+-----+
|location                                 |county|zip  |
+-----------------------------------------+------+-----+
|Amridge University                       |1101  |36117|
|Central Alabama Community College        |1037  |35010|
|Enterprise State Community College       |1035  |36330|
|James H. Faulkner State Community College|1003  |36507|
|J F Ingram State Technical College       |1001  |36022|
+-----------------------------------------+------+-----+
only showing top 5 rows

root
 |-- countyId: integer (nullable = true)
 |-- county: string (nullable = true)
 |-- pop2017: integer (nullable = true)
 |-- location: string (nullable = true)
 |-- zip: integer (nullable = true)

+--------+---------------------------+-------+-----------------------------------------+-----+
|countyId|county                     |pop2017|location                                 |zip  |
+--------+---------------------------+-------+-----------------------------------------+-----+
|12027   |DeSoto County, Florida     |36862  |null                                     |null |
|40107   |Okfuskee County, Oklahoma  |12140  |Wes Watkins Technology Center            |74883|
|8105    |Rio Grande County, Colorado|11301  |null                                     |null |
|15003   |Honolulu County, Hawaii    |988650 |Waianae Coast Comprehensive Health Center|96792|
|15003   |Honolulu County, Hawaii    |988650 |Golf Academy of America                  |96744|
+--------+---------------------------+-------+-----------------------------------------+-----+
only showing top 5 rows
```

## 4, Some diffcult case

### `element_at` index start from 1
```
df.withColumn("addressElements", split(col("Address"), " ")).
      withColumn("addressElementCount", size(col("addressElements"))).
      withColumn("zip9", element_at(col("addressElements"), col("addressElementCount")))
```