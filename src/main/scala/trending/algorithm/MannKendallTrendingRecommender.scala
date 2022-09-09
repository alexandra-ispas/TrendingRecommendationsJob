package trending.algorithm

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import trending.algorithm.TrendingAlgorithmComputeUtil.computeMannKendallScore
import trending.domain._

class MannKendallTrendingRecommender(implicit spark: SparkSession) {

  import spark.implicits._

  def compute(dataFrame: DataFrame): DataFrame = {

    val frameWithCountsArray: DataFrame = dataFrame
      .groupBy(IMSORGID, PRODUCTITEMID, COMMERCE)
      .agg(sort_array(collect_list($"counts_and_time")) as "sorted_counts")
      .withColumn("sorted_counts", col("sorted_counts.count"))

    frameWithCountsArray.show(false)

    val computeMannKendallScoreUDF: UserDefinedFunction = udf(computeMannKendallScore)

    frameWithCountsArray
      .withColumn("mann_kendall_sum", computeMannKendallScoreUDF(col("sorted_counts")))
      .withColumn("sum_counts", aggregate(col("sorted_counts"), lit(0.toLong), (x, y) => x + y))
      .withColumn(SCORE, col("sum_counts") * col("mann_kendall_sum"))
      .withColumn("struct", struct(col(SCORE) as SCORE, col(PRODUCTITEMID) as PRODUCTITEMID))
      .groupBy(COMMERCE, IMSORGID)
      .agg(sort_array(collect_list(col("struct")), asc = false) as "tuple")
      .withColumn(TRENDING_RESULT, transform($"tuple", (x: Column) => struct(x.getField(PRODUCTITEMID),
        x.getField(SCORE))))
      .drop(col("tuple"))
  }
}

object MannKendallTrendingRecommender {
  def apply()(implicit spark: SparkSession) = new MannKendallTrendingRecommender()
}