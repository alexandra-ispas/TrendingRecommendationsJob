package trending.algorithm

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import trending.algorithm.TrendingAlgorithmComputeUtil.generateWindowValues
import trending.domain._

import java.sql.Timestamp

class DataPreprocessor(windowSize: String, watchingInterval: String)(implicit spark: SparkSession) {

  def dataPreProcessor(dataFrame: DataFrame): DataFrame = {

    val minTimestamp = spark.sql(s"SELECT current_timestamp() - INTERVAL $watchingInterval AS timestamp")
      .head().getAs[Timestamp](TIMESTAMP)

    println("min time = " +  minTimestamp)

    val eventWithCounts: DataFrame = dataFrame
      .filter(col(TIMESTAMP).gt(minTimestamp))
      .groupBy(
        window(col(TIMESTAMP), windowSize) as WINDOWCOL,
        col(IMSORGID),
        col(PRODUCTITEMID),
        col(COMMERCE))
      .agg(count(col(ID)) as COUNTCOL)

    val timeColumn: DataFrame = generateWindowValues(windowSize, eventWithCounts)

    eventWithCounts
      .withColumn("counts_per_window", struct(col(COUNTCOL), col("window.start") as TIMESTAMP))
      .groupBy(
        col(IMSORGID),
        col(PRODUCTITEMID),
        col(COMMERCE)
      )
      .agg(collect_list("counts_per_window") as "windows")
      .join(timeColumn)
      .withColumn("to_be_added", filter(col("Date"), x => !array_contains(col("windows.timestamp"), x)))
      .withColumn("aux", transform(col("to_be_added"), (x: Column) => struct(lit(0.toLong) as COUNTCOL, x as TIMESTAMP)))
      .withColumn("result", concat(col("aux"), col("windows")))
      .withColumn("counts_and_time", explode(col("result")))
      .drop(col("result"))
      .drop("aux")
      .drop("windows")
      .drop(col("to_be_added"))
      .drop("Date")
  }

}

object DataPreprocessor {
  def apply(windowSize: String, watchingInterval: String)(implicit sparkSession: SparkSession): DataPreprocessor = new DataPreprocessor(windowSize, watchingInterval)
}