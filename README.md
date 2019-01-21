# Luxoft_solution

Hello Team,

Solution_final.txt is the solution for your requirement 

I have tested for below data.

Input data:

Leader-1
-------------------
sensor_id,humidity
s1,10
s2,88
s1,NaN
s3,NaN

Leader-2
----------------------
sensor_id,humidity
s2,80
s3,NaN
s2,78
s1,98

output
-----------------------

scala> val dir="ravi/sensor_ravi/"
dir: String = ravi/sensor_ravi/

scala>

scala> import org.apache.spark.sql.types._
import org.apache.spark.sql.types._

scala>

scala> val customSchema = StructType(Array(StructField("sensor_id", StringType, true),StructField("humidity", FloatType, true)))
customSchema: org.apache.spark.sql.types.StructType = StructType(StructField(sensor_id,StringType,true), StructField(humidity,FloatType,true))

scala>

scala> val sensor_df = spark.read.option("header",true).schema(customSchema).csv(dir+"*.csv")
sensor_df: org.apache.spark.sql.DataFrame = [sensor_id: string, humidity: float]

scala>

scala> val non_nan_df = sensor_df.filter(isnan('humidity) === false).groupBy(col("sensor_id")).agg(min(col("humidity")).as("min_value"),max(col("humidity")).as("max_value"),avg(col("humidity")).as("avg_value")).sort("avg_value")
non_nan_df: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [sensor_id: string, min_value: float ... 2 more fields]

scala>

scala> val nan_df = sensor_df.as("table1").join(
     | non_nan_df.as("table2"),
     | $"table1.sensor_id" === $"table2.sensor_id",
     | "leftanti").groupBy($"table1.sensor_id").agg(min($"table1.humidity").as("min"), max($"table1.humidity").as("max"), avg($"table1.humidity").as("avg")).sort("avg")
nan_df: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [sensor_id: string, min: float ... 2 more fields]

scala> val num_files_processed = sensor_df.inputFiles.length
num_files_processed: Int = 2

scala>

scala> val num_processed_measurements = sensor_df.count
num_processed_measurements: Long = 8

scala>

scala> val num_failed_measurements = sensor_df.filter(isnan('humidity)).count
num_failed_measurements: Long = 3

scala>

scala> val result = non_nan_df.union(nan_df)
result: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [sensor_id: string, min_value: float ... 2 more fields]

scala> result.collect
res1: Array[org.apache.spark.sql.Row] = Array([s1,10.0,98.0,54.0], [s2,78.0,88.0,82.0], [s3,NaN,NaN,NaN])

scala>



Let me know if you need any changes for this solution.
