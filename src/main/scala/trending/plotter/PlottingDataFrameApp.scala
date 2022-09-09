package trending.plotter

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoders, SparkSession}
import trending.algorithm.DataPreprocessor
import trending.data.SparkDataFrameReader.getDataFrame
import trending.domain._
import trending.plotter.PlottingDataFrame.plotDataFrame
import trending.util._

object PlottingDataFrameApp extends App {
  val sparkSessionManager = SparkSessionManager("PlottingDataframe")
  implicit val spark: SparkSession = sparkSessionManager.spark
  implicit val dateFrameSchema: StructType = Encoders.product[InteractionEvent].schema

  val eventDF = getDataFrame(args(0))
  val preprocessedData = DataPreprocessor(args(1), args(2)).dataPreProcessor(eventDF)
  plotDataFrame(preprocessedData)
}