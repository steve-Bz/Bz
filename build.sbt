ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"


lazy val root = (project in file("."))
  .settings(
    name := "mediahub"
  )
val ScalaTestVersion = "3.1.4"
val AkkaVersion = "2.7.0"
val SprayJsonVersion = "1.3.6"
val AlpakkaVersion = "5.0.0"
val AlpakkaKafkaVersion = "4.0.0"
val AkkaHttpVersion = "10.5.0"
val AkkaDiagnosticsVersion = "2.0.0-M4"
val ScalaTest = "3.2.15"
libraryDependencies ++= Seq(
  "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "5.0.0",
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  "io.spray" %% "spray-json" % SprayJsonVersion,
  "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
  "org.scalatest" %% "scalatest" % ScalaTest % Test,
  "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test
  // "org.scalatestplus" %% "scalacheck-1-17" % ScalaTest % Test


)