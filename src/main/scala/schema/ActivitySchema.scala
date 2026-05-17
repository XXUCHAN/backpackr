package schema

import org.apache.spark.sql.types._

object ActivitySchema {
  val EventTimePattern: String = "yyyy-MM-dd HH:mm:ss 'UTC'"

  val rawSchema: StructType = StructType(
    Seq(
      StructField("event_time", StringType, nullable = true),
      StructField("event_type", StringType, nullable = true),
      StructField("product_id", LongType, nullable = true),
      StructField("category_id", LongType, nullable = true),
      StructField("category_code", StringType, nullable = true),
      StructField("brand", StringType, nullable = true),
      StructField("price", DoubleType, nullable = true),
      StructField("user_id", LongType, nullable = true),
      StructField("user_session", StringType, nullable = true)
    )
  )

  val rawPayloadFieldNames: Seq[String] =
    Seq(
      "event_time",
      "event_type",
      "product_id",
      "category_id",
      "category_code",
      "brand",
      "price",
      "user_id",
      "raw_user_session"
    )
}
