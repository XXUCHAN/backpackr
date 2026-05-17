package com.ecommerce.activity.transform

import org.apache.spark.sql.DataFrame

object Sessionizer {
  def apply(df: DataFrame): DataFrame = df
}
