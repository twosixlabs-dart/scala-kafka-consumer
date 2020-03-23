import sbt._

object Dependencies {

    val slf4jVersion = "1.7.20"
    val logbackVersion = "1.2.3"
    val scalaTestVersion = "3.0.5"
    val betterFilesVersion = "3.8.0"
    val embeddedKafkaVersion = "2.2.1"
    val kafkaVersion = "2.2.1"
    val jacksonVersion = "2.9.9"

    val logging = Seq( "org.slf4j" % "slf4j-api" % slf4jVersion,
                       "ch.qos.logback" % "logback-classic" % logbackVersion )

    val betterFiles = Seq( "com.github.pathikrit" %% "better-files" % betterFilesVersion )

    val kafka = Seq( "org.apache.kafka" % "kafka-clients" % kafkaVersion )

    val embeddedKafka = Seq( "io.github.embeddedkafka" %% "embedded-kafka" % embeddedKafkaVersion % "test",
                             "jakarta.ws.rs" % "jakarta.ws.rs-api" % "2.1.2" % "test" ) //https://github.com/sbt/sbt/issues/3618

    val scalaTest = Seq( "org.scalatest" %% "scalatest" % scalaTestVersion % "test" )

    val jackson = Seq( "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion )

}
