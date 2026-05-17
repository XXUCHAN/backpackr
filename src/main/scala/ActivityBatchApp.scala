import config.AppConfigParser
import support.SparkSessionFactory

object ActivityBatchApp {
  def main(args: Array[String]): Unit = {
    val config = AppConfigParser.parse(args)
    val spark = SparkSessionFactory.create(config.appName)

    try {
      spark.sparkContext.setLogLevel("WARN")
      println(s"Initialized batch scaffold for mode=${config.mode.entryName}, runId=${config.runId}")
    } finally {
      spark.stop()
    }
  }
}
