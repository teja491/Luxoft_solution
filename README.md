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


Let me know if you need any changes for this solution.
