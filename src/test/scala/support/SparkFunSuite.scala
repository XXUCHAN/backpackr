package support

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

trait SparkFunSuite extends AnyFunSuite with BeforeAndAfterAll {
  private var sparkInstance: Option[SparkSession] = None

  protected def spark: SparkSession =
    sparkInstance.getOrElse {
      val created = TestSparkSessionFactory.create(getClass.getSimpleName)
      sparkInstance = Some(created)
      created
    }

  protected def stopSparkSession(): Unit = {
    sparkInstance.foreach(_.stop())
    sparkInstance = None
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  override protected def afterAll(): Unit = {
    stopSparkSession()
    super.afterAll()
  }
}
