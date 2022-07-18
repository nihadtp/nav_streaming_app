package org.mutualFund.dataStructure

/** Common Message format for both valid and invalid data.
  * @param isValid if the payload is valid or not
  * @param payload valid string
  * @param garbage invalid string
  * @param exception exception if the string is invalid
  */
case class InputMessage(
    isValid: Boolean,
    payload: Payload = null,
    garbage: String = null,
    exception: Exception = null
)
