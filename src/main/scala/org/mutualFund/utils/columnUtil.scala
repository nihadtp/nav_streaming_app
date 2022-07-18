package org.mutualFund.utils

import org.apache.spark.sql.DataFrame

object columnUtil {

  /** Replace any non alpha-numeric characters with an underscore in all columns in a given dataframe.
    * @param df
    * @return
    */
  def rename(df: DataFrame): DataFrame = {
    val cols = df.columns
    cols.foldLeft(df)((d, col) => {
      d.withColumnRenamed(col, col.replaceAll("\\W+", "_"))
    })

  }
}
