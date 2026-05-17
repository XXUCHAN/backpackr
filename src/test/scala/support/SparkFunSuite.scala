package support

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

trait SparkFunSuite extends AnyFunSuite with BeforeAndAfterAll {
  protected lazy val spark: SparkSession = TestSparkSessionFactory.create(getClass.getSimpleName)

  override protected def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }
}
