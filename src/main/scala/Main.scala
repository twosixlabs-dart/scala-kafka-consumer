import java.util.Properties

import better.files.Resource
import com.worldmodelers.kafka.consumer.scala.ExampleConsumer

object Main {

    def main( args : Array[ String ] ) : Unit = {
        val properties : Properties = {
            val p = new Properties()
            val pis = Resource.getAsStream( f"${args( 0 )}.properties" )
            p.load( pis )
            p
        }

        val topic = properties.getProperty( "topic.from" )
        val persistenceDir = properties.getProperty( "consumer.persist.dir" )

        val consumer = new ExampleConsumer( topic, persistenceDir, properties )
        consumer.run()
    }

}
