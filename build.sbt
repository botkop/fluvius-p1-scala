// The simplest possible sbt build file is just one line:

scalaVersion := "2.13.7"

name := "fluvius-p1-scala"
organization := "be.botkop"
version := "1.0"

enablePlugins(PackPlugin)

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"

libraryDependencies += "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4"


val AkkaVersion = "2.6.14"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % AkkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % AkkaVersion
libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-mqtt" % "3.0.3"

val AkkaHttpVersion = "10.2.7"
libraryDependencies += "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion

libraryDependencies +=  "net.liftweb" %% "lift-json" % "3.4.3"

libraryDependencies += "com.typesafe" % "config" % "1.4.2"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.3" % Runtime
libraryDependencies += "org.slf4j" % "slf4j-jdk14" % "1.7.36" 


val json4sNative = "org.json4s" %% "json4s-native" % "4.0.3"
libraryDependencies += json4sNative

