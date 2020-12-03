// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// Read from CSV and drop comments and empty rows above from actual turbine data.
var df = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load("/FileStore/tables/turbine_power_data.csv")
df = df.withColumn("index", monotonically_increasing_id()).filter("index > 6").drop("index")
/* Remove rows with null, empty cells and corrupt data (non-integers like find_me)
Total records before removing nulls 26,633
Removing whole row, if null value or empty cells present in any cloumn except _c0 column.*/
df = df.na.drop(how="any")
//Remove rows with 'null' value in '_c0' column
df = df.filter("_c0 != 'null'") // 30 rows with null value in '_c0' column
//Total records after removing nulls 26,547
//Remove corrupt data i.e. non-numeric records like 'find_me' from any column.
val expr = "\\d+"
df = df.filter(col("_c1").rlike(expr) && col("_c2").rlike(expr) && col("_c3").rlike(expr))
//Total records after removing non-numeric cell values from _c1 26,486
//convert time column to timestamp and power values to double.
df =  df.withColumn("time", unix_timestamp(col("_c0"), "M/d/yyyy HH:mm").cast("timestamp")) 
        .withColumn("turbine1", col("_c1").cast(DoubleType)).withColumn("turbine2", col("_c2").cast(DoubleType))
        .withColumn("turbine3", col("_c3").cast(DoubleType)).drop("_c0","_c1","_c2","_c3") 

df.show()

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, col, explode, lit, struct}
//Convert columns to rows
def to_explode(df: DataFrame, by: Seq[String]): DataFrame = {
    // Filter dtypes and split into column names and type description
    val (cols, types) = df.dtypes.filter{ case (c, _) => !by.contains(c)}.unzip
    // Spark SQL supports only homogeneous columns
    require(types.distinct.size == 1, s"${types.distinct.toString}.length != 1")  
    // Create and explode an array of (column_name, column_value) structs    
    val kvs = explode(array(     
      cols.map(c => struct(lit(c).alias("turbine"), col(c).alias("power"))): _*
    ))
  
    val byExprs = by.map(col(_))
    df.select(byExprs :+ kvs.alias("kvs"): _*).select(byExprs ++ Seq($"kvs.turbine", $"kvs.power"): _*)   
}

df = to_explode(df, Seq("time"))     
df.show(60)

// COMMAND ----------

import org.apache.spark.sql.Column
// Resample the data at 10 min granularity for each turbine.
def resample(column: Column, agg_interval: Int): Column = {  
    // Convert the timestamp to unix timestamp format.
    // Unix timestamp = number of seconds since 00:00:00 UTC, 1 January 1970.
    val col_ut =  unix_timestamp(column, "yyyy-MM-dd HH:mm:ss")            
    val col_ut_agg =  floor(col_ut / agg_interval) * agg_interval
    // Convert to and return a human readable timestamp
    return from_unixtime(col_ut_agg).cast("timestamp")
}   
 
df = df.withColumn("time_10Min_resampled", resample(col("time"), 600)).drop(col("time")) //79548 rows
df.show(60)


// COMMAND ----------

//Sum the power value of all 3 turbines and get the Farm level power at 10 min granularity.
//Using GroupBy is not good for performance. Use Windows functions.
val df_grpby_farmPowerSum = df.groupBy("time_10Min_resampled").agg(sum("power").alias("farmpowersum"))
println("Total Rows : " + df_grpby_farmPowerSum.count())
df_grpby_farmPowerSum.show(5) 

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
//Sum the power value of all 3 turbines and get the Farm level power at 10 min granularity.
val window10MSpecPartition = Window.partitionBy($"time_10Min_resampled").orderBy(col("time_10Min_resampled").desc) 
var df_farmPowerSum = df.withColumn("farmpowersum",sum(col("power")).over(window10MSpecPartition))
//GroupBy gives only one row for aggregated sum. But windows function shows all rows, which are duplicates and not required, so dropping them.
df_farmPowerSum = df_farmPowerSum.drop("turbine","power").dropDuplicates()
println("Total Rows : " + df_farmPowerSum.count())
df_farmPowerSum.show()

