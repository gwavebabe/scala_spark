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

object dtbExample4 {
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

		val inputDS = streamingInputDF.as[ event ]
		val streamingCountsDF = inputDS.groupBy( $"action", window( $"time", "3 minutes" ) )
			.count()
		sparkSession.conf.set( "spark.sql.shuffle.partitions", "1" )
		streamingCountsDF.createOrReplaceTempView( "counts" )

		// "select action, window.end as time, count from counts order by time, action"
		var sc1: SelectCreator = new SelectCreator()
		var sql1 = sc1
			.column( "action" )
			.column( "window.end as time" )
			.column( "count" )
			.from( "counts" )
			.orderBy( "time" )
			.orderBy( "action" )
			.getBuilder
			.toString()
		//			sc1.ppcs.setParameter(name, value)
		var params = sc1.ppsc.getParameterMap
		if ( params.size() != 0 ) {
			for ( i ← 0 to params.size - 1 ) {
				var name = "param" + i.toString()
				sql1 = sql1.replaceAll( ":" + name, params.get( name ).toString() )
			}
		}

				// "select action, window.end as time, count from counts order by time, action"
		var sc12: SelectCreator = new SelectCreator()
		var sql2 = sc12
			.column( "*" )
			.from( "counts" )
			.getBuilder
			.toString()
		var params2 = sc12.ppsc.getParameterMap
		if ( params2.size() != 0 ) {
			for ( i ← 0 to params2.size - 1 ) {
				var name = "param" + i.toString()
				sql2 = sql2.replaceAll( ":" + name, params.get( name ).toString() )
			}
		}

		var nodeQueryDF = sparkSession.sql( sql1 )
//		var nodeQueryDF = sparkSession.sql( sql2 )

		val doorsDF = nodeQueryDF
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