import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoders, SparkSession}
import trending.algorithm.{DataPreprocessor, MannKendallTrendingRecommender}
import trending.cli.{TrendingRecommendationsArgs, TrendingRecommendationsArgsParser}
import trending.data.SparkDataFrameReader._
import trending.domain._
import trending.plotter.PlottingDataFrame.plotDataFrame
import trending.producer.DataGenerator._
import trending.util.SparkSessionManager

object TrendingRecommendations {

  def main(args: Array[String]): Unit = {
    val generatorArgs: TrendingRecommendationsArgs = TrendingRecommendationsArgsParser.parseArgs(args)

    val sparkSessionManager = SparkSessionManager("TrendingRecommendations")
    implicit val spark: SparkSession = sparkSessionManager.spark
    implicit val dateFrameSchema: StructType = Encoders.product[InteractionEvent].schema

    generateValues(generatorArgs.numberOfGeneratedIntervals, generatorArgs.generatedWindowSize)

    val eventDF = getDataFrame(INPUT_FILE)
    val preprocessedData = DataPreprocessor(generatorArgs.windowSize, generatorArgs.watchingInterval).dataPreProcessor(eventDF)

    if (generatorArgs.isPlotted)
      plotDataFrame(preprocessedData)

    val finalTrendingResult = MannKendallTrendingRecommender().compute(preprocessedData)

    finalTrendingResult.show(truncate = false)

    sparkSessionManager.closeSession()
  }
}
