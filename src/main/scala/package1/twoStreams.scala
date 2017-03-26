// To run this example, start first the node program genAndWrite2HDFS_DTB_exampleStructured.js.

package package1
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.sql.Timestamp
import ca.krasnay.sqlbuilder.{ SelectCreator }

import org.springframework.jdbc.core.JdbcTemplate
import ca.krasnay.sqlbuilder.Predicates.and
import ca.krasnay.sqlbuilder.Predicates.{ eq ⇒ eqSQL }
import ca.krasnay.sqlbuilder.Predicates.exists
import ca.krasnay.sqlbuilder.Predicates.in
import ca.krasnay.sqlbuilder.Predicates.not

object twoStreams {
	case class event( time: Timestamp, action: String )

	def main( args: Array[ String ] ) {

		val inputPath = "hdfs://localhost:8020/user/hduser1/sparkExamples/output/databricks_1/"

		val sparkSession = SparkSession
			.builder
			.master( "local[3]" )
			.appName( "time window example" )
			.getOrCreate()
		import sparkSession.implicits._
		val jsonSchema = new StructType().add( "time", TimestampType ).add( "action", StringType )

		val streamingInputDF =
			sparkSession
				.readStream
				.schema( jsonSchema )
				.option( "maxFilesPerTrigger", 1 )
				.json( inputPath )

		val inputDS = streamingInputDF.as[ event ].filter($"action" === "close")

		val streamingCountsDF = inputDS.groupBy( $"action", window( $"time", "3 minutes" ) )
			.count()
			//val deviceEventsDS = ds.select($"device_name", $"cca3", $"c02_level").where($"c02_level" > 1300)
		// "select action, window.end as time, count from counts order by time, action"
		var nodeQueryDS =	streamingCountsDF.select("action", "window", "count")
//		.orderBy("time", "action")

		val doorsDF = nodeQueryDS
			.writeStream
			//			.format("memory")
			//			.queryName( "counts" )
			.outputMode( "complete" )
			.format( "console" )
			.option( "truncate", false )
			.option( "numRows", 1000 )
			.start()


			
			
//open
		val inputDS1 = streamingInputDF.as[ event ].where($"action" === "open")

		val streamingCountsDF1 = inputDS1.groupBy( $"action", window( $"time", "3 minutes" ) )
			.count()
			//val deviceEventsDS = ds.select($"device_name", $"cca3", $"c02_level").where($"c02_level" > 1300)
		// "select action, window.end as time, count from counts order by time, action"
		var nodeQueryDS1 =	streamingCountsDF1.select("action", "window", "count")
//		.orderBy("time", "action")

		val doorsDF1 = nodeQueryDS1
			.writeStream
			//			.format("memory")
			//			.queryName( "counts" )
			.outputMode( "complete" )
			.format( "console" )
			.option( "truncate", false )
			.option( "numRows", 1000 )
			.start()

			
			
			
			
		Thread.sleep( 5000 )
		doorsDF.awaitTermination()

	}
}