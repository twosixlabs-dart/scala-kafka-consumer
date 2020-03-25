package com.worldmodelers.kafka.consumer.scala

import java.time.Duration
import java.util.Properties

import better.files.File
import com.worldmodelers.kafka.messages.ExampleConsumerMessage
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecords, KafkaConsumer}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

class ExampleConsumer( val topic : String, val persistDir : String, val properties : Properties ) {

    implicit val executionContext : ExecutionContext = scala.concurrent.ExecutionContext.global

    private val LOG : Logger = LoggerFactory.getLogger( getClass )
    private val kafkaProps : Properties = getKafkaProperties()

    protected lazy val consumer : Consumer[ String, ExampleConsumerMessage ] = {
        val c = new KafkaConsumer[ String, ExampleConsumerMessage ]( kafkaProps )
        c.subscribe( List( topic ).asJava )
        c
    }

    private def persist( message : ExampleConsumerMessage ) : Unit = {
        val updatedMessage = message.copy( breadcrumbs = message.breadcrumbs :+ "scala-kafka-consumer" )
        val file = File( s"${persistDir}/${updatedMessage.id}.txt" )
        file.appendLine( s"${updatedMessage.id}" )
        file.appendLine( s"${updatedMessage.breadcrumbs.mkString( "," )}" )
    }

    def run( ) : Unit = {
        LOG.info( "starting kafka consumer" )
        try {
            while ( true ) {
                consumer.subscribe( List( topic ).asJava )
                val records : ConsumerRecords[ String, ExampleConsumerMessage ] = consumer.poll( Duration.ofMillis( kafkaProps.getProperty( "poll.timeout.millis" ).toInt ) )
                records.asScala.foreach( message => {
                    LOG.info( s"persisting message : ${message.value.id}" )
                    persist( message.value() )
                } )

            }
        }
        finally stop()
    }

    def stop( ) : Unit = consumer.close()

    private def getKafkaProperties( ) : Properties = {
        val kProps = properties
          .asScala
          .toList
          .filter( _._1.startsWith( "kafka." ) )
          .map( pair => {
              val key = pair._1.split( "kafka." )( 1 )
              (key, pair._2)
          } )

        val kafkaProps = new Properties()
        kProps.foreach( prop => kafkaProps.put( prop._1, prop._2 ) )
        kafkaProps
    }

}