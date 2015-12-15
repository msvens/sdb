import sbt._
import Keys._

object Dependencies {

  val luceneVersion = "5.3.1"
  val json4sVersion = "3.2.10"

  //for testing
  val junit = "junit" % "junit" % "4.11"
  val scalatest = "org.scalatest" %% "scalatest" % "2.2.4"
  val pegdown = "org.pegdown" % "pegdown" % "1.4.2"

  val json4s = "org.json4s" %% "json4s-native" % json4sVersion
  val json4sext = "org.json4s" %% "json4s-ext" % json4sVersion

  val jsonDeps = Seq(json4s, json4sext)

  val akka = "com.typesafe.akka" %% "akka-actor" % "2.3.9"
  val nscalaTime = "com.github.nscala-time" %% "nscala-time" % "1.4.0"
  val mcore = "org.mellowtech" % "core" % "3.0.3"

  val luceneCore = "org.apache.lucene" % "lucene-core" % luceneVersion
  val luceneAnalyzers = "org.apache.lucene" % "lucene-analyzers-common" % luceneVersion
  val luceneQueryParser = "org.apache.lucene" % "lucene-queryparser" % luceneVersion

  val luceneDeps = Seq(luceneCore, luceneAnalyzers, luceneQueryParser)

  //val scalaCompiler = scalaVersion("org.scala-lang" % "scala-compiler" % _)
  val scalaCompiler = "org.scala-lang" % "scala-compiler" % "2.11.7"
}