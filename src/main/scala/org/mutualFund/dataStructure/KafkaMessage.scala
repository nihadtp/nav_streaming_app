package org.mutualFund.dataStructure

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.log4j.{Level, Logger}
import java.time.LocalDateTime

/** Kafka producer requires 'value' field as a mandatory field. Rest is optional like key, offset etc.
  * 'topic' field can be either provided in the dataframe or can be passed as an option in Kafka-Spark connector.
  * I chose to provide it in Dataframe.
  * @param value
  * @param topic
  */
case class KafkaMessage(value: String, topic: String)

object KafkaMessage {
  val logger = Logger.getLogger(KafkaMessage.getClass)
  logger.setLevel(Level.INFO)

  def apply(
      data: String,
      exception: String,
      mapper: ObjectMapper,
      topic: String
  ): KafkaMessage = {
    logger.info(s"Corrupted data is $data and exception is $exception.")
    val now = LocalDateTime.now().toString
    val source = "prodigal_streaming_app"
    val metadata = MetaData(now, source)
    val exceptionBody = ExceptionBody(data, exception)
    val exceptionMessage = ExceptionMessage(metadata, exceptionBody)
    val value = mapper.writeValueAsString(exceptionMessage)
    KafkaMessage(value, topic)
  }
}
