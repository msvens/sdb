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

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.10"

libraryDependencies += "org.json4s" %% "json4s-ext" % "3.2.10"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.9"

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "1.4.0"


libraryDependencies += "junit" % "junit" % "4.11" % "test"
libraryDependencies += "org.mellowtech" % "core" % "3.0.3"

//lucene
libraryDependencies ++= Seq(
  "org.apache.lucene" % "lucene-core" % "5.3.1",
  "org.apache.lucene" % "lucene-analyzers-common" % "5.3.1",
  "org.apache.lucene" % "lucene-queryparser" % "5.3.1"
)
