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

object dtbExample6 {
org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)
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

		//		val inputDS = streamingInputDF.as[ event ].filter($"action" === "close")
		val inputDS = streamingInputDF.as[ event ].filter( _.action.equalsIgnoreCase( "close" ) )
		import org.apache.spark.sql.functions._
		import org.apache.spark.sql.expressions.scalalang.typed
		val runningCountDS = inputDS.groupByKey( _.action ).agg( typed.count[ event ]( _.action ) )
		//		                     dataset.groupByKey(_.productId).agg(typed.sum[Token](_.score)).show

		val streamingCountsDF = inputDS.groupBy( $"action", window( $"time", "3 minutes" ) )
			.count()
		//val deviceEventsDS = ds.select($"device_name", $"cca3", $"c02_level").where($"c02_level" > 1300)
		// "select action, window.end as time, count from counts order by time, action"
		var nodeQueryDS_close = streamingCountsDF.select( "action", "window", "count" )
		//		.orderBy("time", "action")

		val doorsDF_close_out = nodeQueryDS_close
			.writeStream
			.format( "console" )
			.queryName( "close_counts" )
			.outputMode( "complete" )
			.start()

		doorsDF_close_out.awaitTermination()
		
		
	}
}