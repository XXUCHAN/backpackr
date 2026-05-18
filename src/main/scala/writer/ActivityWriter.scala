package writer

import org.apache.spark.sql.DataFrame
import support.PathBuilder

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, Paths, SimpleFileVisitor, StandardCopyOption}

object ActivityWriter {
  def writeToStaging(df: DataFrame, stagingPath: String): Unit =
    df.write
      .mode("overwrite")
      .partitionBy("event_date_kst")
      .parquet(stagingPath)

  def promoteToFinal(stagingPath: String, finalBasePath: String, partitions: Seq[String]): Seq[String] = {
    val promotedPaths = partitions.distinct.sorted.map { partitionDate =>
      val sourcePartitionPath = Paths.get(PathBuilder.partitionPath(stagingPath, partitionDate))
      val targetPartitionPath = Paths.get(PathBuilder.partitionPath(finalBasePath, partitionDate))

      require(Files.exists(sourcePartitionPath), s"staging partition path does not exist: $sourcePartitionPath")

      Option(targetPartitionPath.getParent).foreach(parent => Files.createDirectories(parent))
      deletePath(targetPartitionPath)

      try {
        Files.move(sourcePartitionPath, targetPartitionPath, StandardCopyOption.ATOMIC_MOVE)
      } catch {
        case _: Exception =>
          Files.move(sourcePartitionPath, targetPartitionPath)
      }

      targetPartitionPath.toString
    }

    promotedPaths
  }

  def cleanupPath(path: String): Unit =
    deletePath(Paths.get(path))

  private def deletePath(path: Path): Unit = {
    if (Files.notExists(path)) {
      ()
    } else {
      Files.walkFileTree(
        path,
        new SimpleFileVisitor[Path] {
          override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
            Files.deleteIfExists(file)
            FileVisitResult.CONTINUE
          }

          override def postVisitDirectory(dir: Path, exc: java.io.IOException): FileVisitResult = {
            Files.deleteIfExists(dir)
            FileVisitResult.CONTINUE
          }
        }
      )
    }
  }
}
