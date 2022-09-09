package trending.producer

import trending.producer.InteractionEventProducer._
import trending.util.ObjectMapperFactory

import java.sql.Timestamp
import java.time.Instant

object DataGenerator {

  def generateValues(timeIntervals: Int, windowSizeMillis: Long): Unit = {
    val time: Timestamp = Timestamp.from(Instant.now())

    (1 to timeIntervals).foreach {
      generateRandomInteractionEvent(_, time, windowSizeMillis)
    }

    if (timeIntervals > 0)
      ObjectMapperFactory.getWriter.close()
  }
}