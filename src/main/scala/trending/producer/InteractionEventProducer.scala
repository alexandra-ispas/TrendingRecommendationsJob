package trending.producer

import io.jvm.uuid._
import trending.domain.InteractionEvent
import trending.domain.InteractionEvent.toJSON
import trending.util.ObjectMapperFactory

import java.sql.Timestamp
import scala.util.Random


object InteractionEventProducer {

  val randomGenerator = new Random()

  private val imsOrdIds: List[String] = List(
    "E38130325FF373D60A494210@AdobeOrg",
    "F48130325FF373D60A494210@AdobeOrg",
    "G58130325FF373D60A494210@AdobeOrg"
  )

  private val commerceOptions: List[String] = List("purchase", "productView")

  def generateRandomInteractionEvent(intervalIndex: Int, startTime: Timestamp, windowSizeMillis: Long): Unit = {
    val randomUUID = UUID.random
    val randomImsOrdID = imsOrdIds(randomGenerator.nextInt(1))
    val randomProductItemId = randomGenerator.nextInt(3)
    val randomUserId = UUID.random
    val randomCommerce = commerceOptions(randomGenerator.nextInt(2))

    val coefficient = randomGenerator.nextInt(80)

    val time: Long = startTime.getTime + intervalIndex * windowSizeMillis + randomGenerator.nextInt(windowSizeMillis.toInt)
    val timestamp = new Timestamp(time)

    val numberOfEvents: Int = coefficient * Math.sin(time).toInt + coefficient + intervalIndex

    (1 to numberOfEvents).foreach { _ =>
      ObjectMapperFactory
        .getWriter
        .write(toJSON(new InteractionEvent(randomUUID.toString,
          randomImsOrdID,
          randomProductItemId,
          randomUserId.toString,
          randomCommerce,
          timestamp)).noSpaces + "\n")
    }
  }
}
