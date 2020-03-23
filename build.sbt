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