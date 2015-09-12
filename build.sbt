name := """sdb"""

scalaVersion := "2.11.7"

version := "0.1-SNAPSHOT"
organization := "org.mellowtech"

resolvers ++= Seq(
  "Mellowtech Snapshots" at "http://www.mellowtech.org/nexus/content/groups/public"
)

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.10"

libraryDependencies += "org.json4s" %% "json4s-ext" % "3.2.10"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.9"

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "1.4.0"


libraryDependencies += "junit" % "junit" % "4.11" % "test"

libraryDependencies += "org.mellowtech" % "core" % "3.0.1"

//lucene
libraryDependencies ++= Seq(
  "org.apache.lucene" % "lucene-core" % "4.10.3",
  "org.apache.lucene" % "lucene-analyzers-common" % "4.10.3",
  "org.apache.lucene" % "lucene-queryparser" % "4.10.3"
)
