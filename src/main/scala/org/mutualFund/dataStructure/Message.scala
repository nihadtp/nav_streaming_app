package org.mutualFund.dataStructure

/** Message consists of both metadata and body.
  * @param metadata
  * @param body
  */
case class Message(metadata: MetaData, body: Body)
