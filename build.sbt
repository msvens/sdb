import Dependencies._
import sbt.Keys._
import sbt._
import LaikaKeys._

LaikaPlugin.defaults

lazy val buildSettings = Seq(
  version := "0.2-SNAPSOT",
  organization := "org.mellowtech",
  scalaVersion := "2.11.7",
  publishArtifact in Test := false,
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-h", "target/site/test-reports"),
  //LaikaPlugin.defaults,
  includeAPI in Laika := true
)

lazy val commonDeps = Seq(
  scalatest % Test,
  junit % Test,
  pegdown % Test
)

lazy val macroSub = (project in file ("macro")).
  settings(buildSettings: _*).
  settings(
    name := "macro",
    libraryDependencies += scalaCompiler
  )

lazy val core = (project in file ("core")).dependsOn(macroSub).
  settings(
    name := "sdb",
    libraryDependencies ++= commonDeps,
    libraryDependencies ++= luceneDeps,
    libraryDependencies ++= jsonDeps,
    libraryDependencies += akka,
    libraryDependencies += nscalaTime,
    libraryDependencies += mcore,
    publishMavenStyle := true,
    pomIncludeRepository := { _ => false },
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    }
  )

lazy val examples = (project in file ("examples")).dependsOn(core).
  settings(buildSettings: _*).
  settings(
    name:= "sdb-examples",
    libraryDependencies ++= commonDeps
  )

lazy val root = (project in file (".")).aggregate(macroSub, core).
  settings(buildSettings: _*).
  settings(
    publish := false
  )

/*
name := """sdb"""

scalaVersion := "2.11.7"

version := "0.2-SNAPSHOT"

organization := "org.mellowtech"

publishMavenStyle := true

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}


libraryDependencies += "junit" % "junit" % "4.11" % "test"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "org.pegdown" % "pegdown" % "1.4.2" % "test"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.10"

libraryDependencies += "org.json4s" %% "json4s-ext" % "3.2.10"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.9"

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "1.4.0"



libraryDependencies += "org.mellowtech" % "core" % "3.0.3"

libraryDependencies ++= Seq(
  "org.apache.lucene" % "lucene-core" % "5.3.1",
  "org.apache.lucene" % "lucene-analyzers-common" % "5.3.1",
  "org.apache.lucene" % "lucene-queryparser" % "5.3.1"
)

*/
