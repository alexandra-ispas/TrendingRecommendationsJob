package trending.data

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkDataFrameWriter {

  def writeDataFrame(dataFrame: DataFrame, fileName: String)(implicit spark: SparkSession): Unit = {
    dataFrame.write
      .mode(SaveMode.Overwrite)
      .json(fileName)
  }
}
