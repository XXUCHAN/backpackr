package reader

import schema.ActivitySchema
import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvActivityReader {
  def read(spark: SparkSession, inputPath: String): DataFrame =
    spark.read
      .option("header", "true")
      .schema(ActivitySchema.rawSchema)
      .csv(inputPath)
}
