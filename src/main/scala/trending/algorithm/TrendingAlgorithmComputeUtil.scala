package trending.algorithm

import org.apache.spark.sql.functions.{col, max, min}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp

object TrendingAlgorithmComputeUtil {

  val computeMannKendallScore: Array[Long] => Int = (countsList: Array[Long]) =>
    (0 to countsList.length - 2)
      .foldLeft(0)((acc, k) => acc + (k + 1 until countsList.length)
        .foldLeft(0)((acc1, elem) => acc1 + Math.signum(countsList(elem) - countsList(k)).toInt))

  def generateWindowValues(timeInterval: String, dataFrame: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val minTimestamp: Timestamp = dataFrame.select(min(col("window.start"))).head().getAs[Timestamp](0)
    val maxTimestamp = dataFrame.select(max(col("window.start"))).head().getAs[Timestamp](0)

    spark
      .sql(s"SELECT sequence(to_timestamp('$minTimestamp'), to_timestamp(' $maxTimestamp'), interval $timeInterval) as Date")
  }
}
