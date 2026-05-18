package writer

import org.apache.spark.sql.DataFrame
import support.PathBuilder

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, Paths, SimpleFileVisitor, StandardCopyOption}

object ActivityWriter {
  private val PromoteBackupDirName = ".promote-backup"
  private val PromoteWorkDirName = ".promote-work"
  private val DefaultPromoteRetryCount = 3

  def writeToStaging(df: DataFrame, stagingPath: String): Unit =
    df.write
      .mode("overwrite")
      .partitionBy("event_date_kst")
      .parquet(stagingPath)

  def promoteToFinal(stagingPath: String, finalBasePath: String, partitions: Seq[String]): Seq[String] =
    promoteToFinal(stagingPath, finalBasePath, partitions, DefaultFileOperations, DefaultPromoteRetryCount)

  private[writer] def promoteToFinal(
      stagingPath: String,
      finalBasePath: String,
      partitions: Seq[String],
      fileOperations: FileOperations,
      maxRetryCount: Int
  ): Seq[String] = {
    require(maxRetryCount >= 1, s"maxRetryCount must be at least 1: $maxRetryCount")

    partitions.distinct.sorted.map { partitionDate =>
      val sourcePartitionPath = Paths.get(PathBuilder.partitionPath(stagingPath, partitionDate))
      val targetPartitionPath = Paths.get(PathBuilder.partitionPath(finalBasePath, partitionDate))
      val backupPath = backupPartitionPath(finalBasePath, partitionDate)
      val workRoot = workPartitionRoot(finalBasePath, partitionDate)

      require(fileOperations.exists(sourcePartitionPath), s"staging partition path does not exist: $sourcePartitionPath")

      Option(targetPartitionPath.getParent).foreach(fileOperations.createDirectories)
      Option(backupPath.getParent).foreach(fileOperations.createDirectories)

      fileOperations.deleteRecursively(backupPath)
      fileOperations.deleteRecursively(workRoot)

      val hadExistingFinalPartition = fileOperations.exists(targetPartitionPath)
      if (hadExistingFinalPartition) {
        moveWithFallback(fileOperations, targetPartitionPath, backupPath)
      }

      try {
        promotePartitionWithRetry(
          sourcePartitionPath = sourcePartitionPath,
          targetPartitionPath = targetPartitionPath,
          workPartitionRoot = workRoot,
          fileOperations = fileOperations,
          maxRetryCount = maxRetryCount
        )
        fileOperations.deleteRecursively(backupPath)
        fileOperations.deleteRecursively(workRoot)
        targetPartitionPath.toString
      } catch {
        case error: Exception =>
          rollbackPartition(
            targetPartitionPath = targetPartitionPath,
            backupPartitionPath = backupPath,
            hadExistingFinalPartition = hadExistingFinalPartition,
            fileOperations = fileOperations
          )
          fileOperations.deleteRecursively(workRoot)
          throw new IllegalStateException(
            s"failed to promote partition after $maxRetryCount attempt(s): $partitionDate",
            error
          )
      }
    }
  }

  def cleanupPath(path: String): Unit =
    DefaultFileOperations.deleteRecursively(Paths.get(path))

  private def promotePartitionWithRetry(
      sourcePartitionPath: Path,
      targetPartitionPath: Path,
      workPartitionRoot: Path,
      fileOperations: FileOperations,
      maxRetryCount: Int
  ): Unit = {
    var lastError: Option[Exception] = None

    (1 to maxRetryCount).foreach { attempt =>
      val workingPartitionPath = workPartitionRoot.resolve(f"attempt-$attempt%02d")

      fileOperations.deleteRecursively(targetPartitionPath)
      fileOperations.deleteRecursively(workingPartitionPath)

      try {
        fileOperations.copyRecursively(sourcePartitionPath, workingPartitionPath)
        moveWithFallback(fileOperations, workingPartitionPath, targetPartitionPath)
        return
      } catch {
        case error: Exception =>
          lastError = Some(error)
          fileOperations.deleteRecursively(targetPartitionPath)
          fileOperations.deleteRecursively(workingPartitionPath)
      }
    }

    throw lastError.getOrElse(new IllegalStateException("promote failed without a recorded error"))
  }

  private def rollbackPartition(
      targetPartitionPath: Path,
      backupPartitionPath: Path,
      hadExistingFinalPartition: Boolean,
      fileOperations: FileOperations
  ): Unit = {
    fileOperations.deleteRecursively(targetPartitionPath)

    if (hadExistingFinalPartition && fileOperations.exists(backupPartitionPath)) {
      Option(targetPartitionPath.getParent).foreach(fileOperations.createDirectories)
      moveWithFallback(fileOperations, backupPartitionPath, targetPartitionPath)
    }
  }

  private def moveWithFallback(fileOperations: FileOperations, source: Path, target: Path): Unit =
    try {
      fileOperations.move(source, target, atomic = true)
    } catch {
      case _: Exception =>
        fileOperations.move(source, target, atomic = false)
    }

  private def backupPartitionPath(finalBasePath: String, partitionDate: String): Path =
    Paths.get(finalBasePath, PromoteBackupDirName, s"event_date_kst=$partitionDate")

  private def workPartitionRoot(finalBasePath: String, partitionDate: String): Path =
    Paths.get(finalBasePath, PromoteWorkDirName, s"event_date_kst=$partitionDate")
}

private[writer] trait FileOperations {
  def exists(path: Path): Boolean
  def createDirectories(path: Path): Unit
  def move(source: Path, target: Path, atomic: Boolean): Unit
  def deleteRecursively(path: Path): Unit
  def copyRecursively(source: Path, target: Path): Unit
}

private[writer] object DefaultFileOperations extends FileOperations {
  override def exists(path: Path): Boolean =
    Files.exists(path)

  override def createDirectories(path: Path): Unit =
    Files.createDirectories(path)

  override def move(source: Path, target: Path, atomic: Boolean): Unit = {
    if (atomic) {
      Files.move(source, target, StandardCopyOption.ATOMIC_MOVE)
    } else {
      Files.move(source, target)
    }
  }

  override def deleteRecursively(path: Path): Unit = {
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

  override def copyRecursively(source: Path, target: Path): Unit = {
    Files.walkFileTree(
      source,
      new SimpleFileVisitor[Path] {
        override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
          val relativePath = source.relativize(dir)
          Files.createDirectories(target.resolve(relativePath))
          FileVisitResult.CONTINUE
        }

        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          val relativePath = source.relativize(file)
          Files.copy(file, target.resolve(relativePath), StandardCopyOption.REPLACE_EXISTING)
          FileVisitResult.CONTINUE
        }
      }
    )
  }
}
