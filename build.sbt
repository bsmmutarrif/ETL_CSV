//ThisBuild / version := "0.1.0-SNAPSHOT"
//
//ThisBuild / scalaVersion := "2.12.17"
//
//lazy val root = (project in file("."))
//  .settings(
//    name := "ETL-License"
//  )

ThisBuild / scalaVersion := "2.12.17"
ThisBuild / version := "0.1.0-SNAPSHOT"
lazy val root = (project in file("."))
  .settings(
    name := "ETL-License",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.4.3",
      "org.apache.spark" %% "spark-sql" % "3.4.3",
      "com.oracle.database.jdbc" % "ojdbc8" % "21.4.0.0.1",
      "com.typesafe" % "config" % "1.4.2"
    )
  )
