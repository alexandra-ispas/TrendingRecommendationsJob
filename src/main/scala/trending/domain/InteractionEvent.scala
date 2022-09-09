package trending.domain

import io.circe.Json

import java.sql.Timestamp

case class InteractionEvent(id: String,
                            imsOrgId: String,
                            productItemId: Int,
                            userId: String,
                            commerce: String,
                            timestamp: Timestamp)

object InteractionEvent {
  def toJSON(event: InteractionEvent): Json = {
    Json.obj(
      ID -> Json.fromString(event.id),
      IMSORGID -> Json.fromString(event.imsOrgId),
      PRODUCTITEMID -> Json.fromInt(event.productItemId),
      USERID -> Json.fromString(event.userId),
      COMMERCE -> Json.fromString(event.commerce),
      TIMESTAMP -> Json.fromString(event.timestamp.toString)
    )
  }
}