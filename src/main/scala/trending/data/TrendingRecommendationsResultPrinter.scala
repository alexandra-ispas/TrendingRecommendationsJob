package trending.data

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}
import trending.domain.{COMMERCE, ID, IMSORGID, SCORE, TRENDING_RESULT}

object TrendingRecommendationsResultPrinter {

  def printResult(dataFrame: DataFrame): Unit = {
    val list = collectResult(dataFrame)
    println(list.foreach(x => println(x._1 + " - " + x._2.mkString("Array(", ", ", ")"))))
  }

  def collectResult(dataFrame: DataFrame): List[((String, String), Array[(Int, Long)])] = {
    dataFrame
      .select(col(COMMERCE), col(IMSORGID), explode(col(TRENDING_RESULT)) as "struct",
        col("struct.id") as ID, col("struct.score") as SCORE)
      .drop(col("struct"))
      .collect()
      .map(row => (row.getAs[String](COMMERCE), row.getAs[String](IMSORGID),
        row.getAs[Int](ID), row.getAs[Long](SCORE)))
      .groupBy(x => (x._1, x._2))
      .map(x => (x._1, x._2.map(w => (w._3, w._4))))
      .toList
  }
}
