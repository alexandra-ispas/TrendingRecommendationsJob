package trending.cli

import scopt.{OParser, OParserBuilder}

object TrendingRecommendationsArgsParser {

  def parseArgs(args: Array[String]): TrendingRecommendationsArgs = {
    import scopt.{DefaultOParserSetup, OParserSetup}

    val setup: OParserSetup = new DefaultOParserSetup {
      override def showUsageOnError: Option[Boolean] = Some(true)
    }

    OParser.parse(buildArgsParser, args, TrendingRecommendationsArgs(), setup) match {
      case Some(config) => config
      case _ => throw new IllegalArgumentException("Error in parsing command line.")
    }
  }

  private def buildArgsParser: OParser[String, TrendingRecommendationsArgs] = {
    implicit val argsParserBuilder: OParserBuilder[TrendingRecommendationsArgs] = OParser.builder[TrendingRecommendationsArgs]
    import argsParserBuilder._

    OParser.sequence(
      opt[String]("windowSize")
        .required()
        .action((x, c) => c.copy(windowSize = x))
        .text("The size of the window for grouping the events."),
      opt[Int]("numberOfGeneratedIntervals")
        .action((x, c) => c.copy(numberOfGeneratedIntervals = x))
        .optional()
        .text("The number of time intervals generated."),
      opt[Boolean]("isPlotted")
        .action((x, c) => c.copy(isPlotted = x))
        .required()
        .text("Choose whether or not to plot the generated data."),
      opt[Long]("generatedWindowSize")
        .action((x, c) => c.copy(generatedWindowSize = x))
        .required()
        .text("Choose the size of the window for the data to be generated."),
      opt[String]("watchingInterval")
        .required()
        .action((x, c) => c.copy(watchingInterval = x))
        .text("The size of the processed time interval.")
    )
  }
}

case class TrendingRecommendationsArgs(
                                        windowSize: String = "",
                                        numberOfGeneratedIntervals: Int = 0,
                                        isPlotted: Boolean = false,
                                        watchingInterval: String = "1 hours",
                                        generatedWindowSize: Long = 60
                                      )
