package org.mutualFund.dataStructure

/** Schema for the Mutual Fund data from the NAV API.
  * @param `Scheme Code`
  * @param `Scheme Name`
  * @param `ISIN Div Payout/ISIN Growth`
  * @param `ISIN Div Reinvestment`
  * @param `Net Asset Value`
  * @param `Scheme Type`
  * @param `Scheme Category`
  * @param `Repurchase Price`
  * @param `Sale Price`
  * @param `Date`
  * @param `Mutual Fund Family`
  */
case class Payload(
    `Scheme Code`: Int,
    `Scheme Name`: String,
    `ISIN Div Payout/ISIN Growth`: Option[String],
    `ISIN Div Reinvestment`: Option[String],
    `Net Asset Value`: Option[String],
    `Scheme Type`: String,
    `Scheme Category`: Option[String],
    `Repurchase Price`: Option[String],
    `Sale Price`: Option[String],
    `Date`: String,
    `Mutual Fund Family`: Option[String]
)
