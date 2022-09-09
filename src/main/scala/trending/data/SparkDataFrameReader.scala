package trending.data

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkDataFrameReader {

  def getDataFrame(fileName: String)(implicit spark: SparkSession, dateFrameSchema: StructType): DataFrame = {
    spark.read
      .schema(dateFrameSchema)
      .json(fileName)
  }
}
