package com.worldmodelers.kafka.messages

import java.util.{Map => JMap}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.serialization.Serdes.WrapperSerde
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

trait ExampleConsumerMessageJsonFormat {

    private val LOG : Logger = LoggerFactory.getLogger( getClass )

    val mapper : ObjectMapper = {
        val m = new ObjectMapper()
        m.registerModule( DefaultScalaModule )
    }

    def unmarshalMessage( json : String ) : Try[ ExampleConsumerMessage ] = {
        try {
            Success( mapper.readValue[ ExampleConsumerMessage ]( json, classOf[ ExampleConsumerMessage ] ) )
        } catch {
            case e : Exception => {
                LOG.error( s"Encountered error unmarshalling json : ${e.getClass} : ${e.getMessage} : ${e.getCause}" )
                e.printStackTrace()
                Failure( e )
            }
        }
    }

    def marshalMessage( message : ExampleConsumerMessage ) : Try[ String ] = {
        try {
            Success( mapper.writeValueAsString( message ) )
        } catch {
            case e : Exception => {
                LOG.error( s"Encountered error marshalling json : ${e.getClass} : ${e.getMessage} : ${e.getCause}" )
                e.printStackTrace()
                Failure( e )
            }
        }
    }

}

class ExampleConsumerMessageSerializer extends Serializer[ ExampleConsumerMessage ] with ExampleConsumerMessageJsonFormat {

    override def configure( configs : JMap[ String, _ ], isKey : Boolean ) : Unit = {}

    override def serialize( topic : String, data : ExampleConsumerMessage ) : Array[ Byte ] = marshalMessage( data ).get.getBytes

    override def close( ) : Unit = {}
}


class ExampleConsumerMessageDeserializer extends Deserializer[ ExampleConsumerMessage ] with ExampleConsumerMessageJsonFormat {

    override def configure( configs : JMap[ String, _ ], isKey : Boolean ) : Unit = {}

    override def deserialize( topic : String, data : Array[ Byte ] ) : ExampleConsumerMessage = unmarshalMessage( new String( data ) ).get

    override def close( ) : Unit = {}
}

class ExampleConsumerMessageSerde extends WrapperSerde[ ExampleConsumerMessage ]( new ExampleConsumerMessageSerializer, new ExampleConsumerMessageDeserializer )