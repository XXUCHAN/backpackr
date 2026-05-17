ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.ecommerce"

lazy val sparkVersion = "3.5.1"

lazy val root = (project in file("."))
  .settings(
    name := "activity-etl-wau",
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-encoding",
      "utf8"
    ),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
      "com.github.scopt" %% "scopt" % "4.1.0",
      "org.scalatest" %% "scalatest" % "3.2.18" % Test
    )
  )
