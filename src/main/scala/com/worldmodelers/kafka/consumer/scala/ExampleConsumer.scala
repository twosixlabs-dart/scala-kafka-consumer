package com.worldmodelers.kafka.consumer.scala

import java.time.Duration
import java.util.Properties

import better.files.File
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecords, KafkaConsumer}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

class ExampleConsumer( val topic : String, val persistDir : String, val properties : Properties ) {

    implicit val executionContext : ExecutionContext = scala.concurrent.ExecutionContext.global

    private val LOG : Logger = LoggerFactory.getLogger( getClass )
    private val kafkaProps : Properties = getKafkaProperties()

    protected lazy val consumer : Consumer[ String, String ] = {
        val c = new KafkaConsumer[ String, String ]( kafkaProps )
        c.subscribe( List( topic ).asJava )
        c
    }

    private def persist( key : String, value : String ) : Unit = {
        val file = File( s"${persistDir}/${key}.txt" )
        file.writeText( value )
    }

    def run( ) : Unit = {
        LOG.info( "starting kafka consumer" )
        try {
            while ( true ) {
                consumer.subscribe( List( topic ).asJava )
                val records : ConsumerRecords[ String, String ] = consumer.poll( Duration.ofMillis( kafkaProps.getProperty( "poll.timeout.millis" ).toInt ) )
                records.asScala.foreach( message => {
                    LOG.info( s"persisting message : ${message.key()}" )
                    persist( message.key(), message.value() )
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
          .withFilter( _._1.startsWith( "kafka." ) )
          .map( pair => {
              val key = pair._1.split( "kafka." )( 1 )
              (key, pair._2)
          } )

        val kafkaProps = new Properties()
        kProps.foreach( prop => kafkaProps.put( prop._1, prop._2 ) )
        kafkaProps
    }

}