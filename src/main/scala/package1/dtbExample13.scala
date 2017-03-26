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
import scala.collection.mutable.ListBuffer
object dtbExample13 {

	def uuid = java.util.UUID.randomUUID.toString

	def currentTimeStampFunc(): java.sql.Timestamp = {
		var r = scala.util.Random
		Thread.sleep( r.nextInt( 2 ) * 1000 )
		var ts = new java.sql.Timestamp(new java.util.Date().getTime )
		ts
	}
	def getEvValueType( arg: String ) = {
		if ( arg.equalsIgnoreCase( "door" ) ) "String" else "Double"
	}
	def getEvValue( arg: String ) = {
		var r = scala.util.Random
		var tempR = scala.util.Random
		if ( arg.equalsIgnoreCase( "door" ) ) { if ( r.nextDouble < 0.5 ) "open" else "close" }
		else {
			if (r.nextInt() < 0.1) String.valueOf( 100 ) else String.valueOf( r.nextInt(100) )
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
		var rand = scala.util.Random
				var listBuf = new ListBuffer[ ( Int, Int, String, Timestamp ) ]()
//		var listBuf = new ListBuffer[ ( Int, Int, String, java.util.Date ) ]()
		for ( i ← 1 to 110 ) {
			listBuf += ( ( i,
				if ( rand.nextInt() < 0.5 ) 123 else 456,
				if ( rand.nextInt() < 0.5 ) "door" else "temp",
				currentTimeStampFunc()
			) )
		}
		var events = listBuf.map( t ⇒ ( t._1, t._2, t._3, t._4, getEvValueType( t._3 ), getEvValue( t._3 ) ) )
		println( events )

		events.foreach { l ⇒
			var r = scala.util.Random
			val sleepTime = r.nextInt( 5 )
			println( sleepTime )
			Thread.sleep( sleepTime * 1000 )

			val eventPath = "hdfs://localhost:8020/user/hduser1/sparkExamples/input/event/event_" + uuid + ".csv"
						var foo = new ListBuffer[ ( Int, Int, String, Timestamp, String, String ) ]()
			foo += ( ( l._1, l._2, l._3, l._4, l._5, l._6 ) )
			spark.createDataset( foo )
				.write.format( "csv" )
				.option( "sep", "," )
				.option( "path", eventPath )
				.save
		}

	}

}