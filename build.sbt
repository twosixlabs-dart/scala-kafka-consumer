import Dependencies._
import sbt._

organization in ThisBuild := "com.worldmodelers.templates"
name := "scala-kafka-consumer"
scalaVersion in ThisBuild := "2.12.7"

resolvers in ThisBuild ++= Seq( "Sonatype releases" at "https://oss.sonatype.org/content/repositories/releases",
                                "Spray IO Repository" at "https://repo.spray.io/",
                                "Maven Central" at "https://repo1.maven.org/maven2/",
                                "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
                                "JCenter" at "https://jcenter.bintray.com" )

lazy val root = ( project in file( "." ) ).settings( libraryDependencies ++= kafka
                                                                             ++ jackson
                                                                             ++ betterFiles
                                                                             ++ logging
                                                                             ++ scalaTest
                                                                             ++ embeddedKafka )

mainClass in(Compile, run) := Some( "Main" )

enablePlugins( JavaAppPackaging )

// don't run tests when build the fat jar, use sbt test instead for that (takes too long when building the image)
test in assembly := {}

assemblyMergeStrategy in assembly := {
    case PathList( "META-INF", "MANIFEST.MF" ) => MergeStrategy.discard
    case PathList( "reference.conf" ) => MergeStrategy.concat
    case x => MergeStrategy.last
