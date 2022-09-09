package trending.producer

import trending.producer.DataGenerator.generateValues

object DataGeneratorApp extends App {
  generateValues(args(0).toInt, args(1).toLong)
}
