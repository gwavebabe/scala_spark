//https://databricks.com/blog/2015/06/02/statistical-and-mathematical-functions-with-dataframes-in-spark.html
//https://spark.apache.org/docs/1.6.0/api/java/org/apache/spark/sql/Column.html
//http://cdn2.hubspot.net/hubfs/438089/notebooks/spark2.0/Apache%20Spark%202.0%20Subqueries.html
// todo: do this https://github.com/jaceklaskowski/mastering-apache-spark-book/blob/master/spark-sql-functions.adoc
package package1
import org.apache.spark.sql.{ DataFrame, SparkSession, Column }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.sql.Timestamp
import ca.krasnay.sqlbuilder.{ SelectCreator }
import org.springframework.jdbc.core.JdbcTemplate
import java.text.SimpleDateFormat
import java.sql.{ Date, Timestamp }
import org.apache.spark.sql.{ Dataset, Encoders }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrameNaFunctions
import org.apache.spark.sql.catalog.Column
import java.util.Date
import scala.collection.mutable.ArrayBuffer
object dtbExample14 {
	private case class Event( id: Int, sensor: Int, sType: String, evTs: String, evValueType: String, evValue: String )
	private case class Event1( id: Int, sensor: Int, sType: String, evTs: Long, evValueType: String, evValue: String )
	private case class Event2( id: Int, sensor: Int, sType: String, evTs: Timestamp, evValueType: String, evValue: String )
	private case class RawInput( id: Int, sensor: Int, sType: String, evTs: Timestamp, evValueType: String, evValue: String )
	private case class State( sName: String, sValue: String, sValueType: String, sTimestamp: Timestamp )
	// node creates event from rules.
	private case class Node(
		nodeType: String, // Could be vending machine node.
		state: State // Use rules to calculate state of certain sensor type.
		//		ts: Timestamp
		)
		
		def tempRule(evValue:String):String = {
			if (evValue.toDouble == 100) "melting" else "ok"
	}
		def doorRule(evValue:String):String = {
			evValue
	}
	// Need to catch exception if it is not digits.
	def stringToTimestamp( s: String ): java.sql.Timestamp = {
		var bi = BigInt( s ) / 1000
		var l = bi.toLong
		var ts = new java.sql.Timestamp( l )
		ts
	}
	//	def toTS( longTS: Long ) = {
	//		new java.sql.Timestamp( longTS )
	//	}
	//	private case class State( sName: String, sValue: String, sValueType: String, sTimestamp: Timestamp )
	//RawInput( id: Int, sensor: Int, sType: String, evTs: Timestamp, evValueType: String, evValue: String )
	private def calState( rawEv: RawInput ): State = {
		rawEv.sType match {
			case "temp" ⇒ State( "temp", tempRule(rawEv.evValue), "String", rawEv.evTs )
			case "door" ⇒ State( "door", doorRule(rawEv.evValue), "String", rawEv.evTs )
		}
	}

	def main( args: Array[ String ] ) {
		val spark = SparkSession
			.builder
			.master( "local[3]" )
			.appName( "BasicDataFrameExample" )
			.getOrCreate()

		import spark.implicits._
		implicit val e_DATE = org.apache.spark.sql.Encoders.DATE // if don't have this one then cannot do map(_.installation) below.
		implicit val e_TIMESTAMP = org.apache.spark.sql.Encoders.TIMESTAMP // if don't have this one then cannot do map(_.installation) below.
		implicit val e_STRING = org.apache.spark.sql.Encoders.STRING // if don't have this one then cannot do map(_.installation) below.

		// todo: https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-sql-structured-streaming.html
		//		var evtSchema = Encoders.product[ Event ].schema
		//		var evtSchema1 = new StructType().add( "time", TimestampType ).add( "action", StringType )
		var evtSchema1 = new StructType()
			.add( "id", IntegerType )
			.add( "sensor", IntegerType )
			.add( "sType", StringType )
			.add( "evTs", StringType )
			.add( "evValueType", StringType )
			.add( "evValue", StringType )

		val evtPath = "hdfs://localhost:8020/user/hduser1/sparkExamples/input/event/*.csv"

		val evtStream = spark.readStream
			.schema( evtSchema1 )
			.option( "maxFilesPerTrigger", 1 )
			.csv( evtPath )
			.as[ Event ]
		//		println( evtStream.isStreaming )
		//	private case class node(
		//		nodeType: String, // Could be vending machine node.
		//		state: State // Use rules to calculate state of certain sensor type.
		//		)
		val foo = evtStream
			.map( l ⇒ RawInput( l.id, l.sensor, l.sType, stringToTimestamp( l.evTs ), l.evValueType, l.evValue ) )
			.map( l ⇒ Node( "vending", calState( l ) ) )

		//		println(foo.schema)
		import org.apache.spark.sql.streaming.OutputMode.{ Complete, Append }

		//		id: Int, sensor: Int, sType: String, evTs: Timestamp, evValueType: String, evValue: String
		//State( sName: String, sValue: String, sValueType: String, sTimestamp: Timestamp )
		//				val query = foo.select( $"nodeType", $"state.sName", $"state.sValue", $"state.sTimestamp")
		//				var out = query.writeStream
		//					.format( "console" )
		//					.outputMode( Append )
		//					.queryName( "test" )
		//					.option( "truncate", false )
		//					.start()

		val query = foo
			.select( $"state.sTimestamp", $"state.sName", $"state.sValue" )
			//			.where( $"sType" === "door" )
			.groupBy(
				window( $"sTimestamp", "5 seconds", "5 seconds" ),
				$"sName", $"sValue"
			).count()
//			.sort($"window.end")
			println(query.schema)	
			query.foreach(l=>println(l))
			
		var out = query.writeStream
			.format( "console" )
			.outputMode( Complete() )
			.queryName( "test" )
			.option( "truncate", false )
			.start()

		out.awaitTermination()

	}

}