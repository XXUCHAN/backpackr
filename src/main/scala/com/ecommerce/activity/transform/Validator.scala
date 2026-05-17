package com.ecommerce.activity.transform

import org.apache.spark.sql.DataFrame

final case class ValidationResult(valid: DataFrame, invalid: DataFrame)

object Validator {
  def apply(df: DataFrame): ValidationResult =
    ValidationResult(valid = df, invalid = df.limit(0))
}
