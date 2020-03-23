package com.worldmodelers.kafka.consumer.scala

import java.util.Properties

import better.files.Resource
import com.worldmodelers.kafka.messages.{ExampleConsumerMessage, ExampleConsumerMessageJsonFormat, ExampleConsumerMessageSerde}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.scalatest.{FlatSpec, Ignore, Matchers}

import scala.concurrent.{ExecutionContext, Future}

@Ignore // TODO - @michael - fix test
class ExampleConsumerTestSuite extends FlatSpec with Matchers with EmbeddedKafka with ExampleConsumerMessageJsonFormat {

    implicit val ec : ExecutionContext = scala.concurrent.ExecutionContext.global
    implicit val config = EmbeddedKafkaConfig( kafkaPort = 6308, zooKeeperPort = 2111 )

    val keySerde : Serde[ String ] = Serdes.String
    implicit val keySerializer = keySerde.serializer()
    implicit val keyDeserializer = keySerde.deserializer()

    val valueSerde : Serde[ ExampleConsumerMessage ] = new ExampleConsumerMessageSerde
    implicit val valueSerializer = valueSerde.serializer()
    implicit val valueDeserializer = valueSerde.deserializer()

    val props : Properties = {
        val p = new Properties()
        val pStream = Resource.getAsStream( "test.properties" )
        p.load( pStream )
        p
    }

    val topicFrom = props.getProperty( "topic.from" )
    val persistDir = props.getProperty( "consumer.persist.dir" )

    "Example Kafka Consumer" should "receive a message" in {
        val exampleConsumer : ExampleConsumer = new ExampleConsumer( topicFrom, persistDir, props )
        withRunningKafka {
            ???
        }
    }

}