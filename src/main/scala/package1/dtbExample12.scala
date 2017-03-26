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
object dtbExample12 {
	private case class Person( id: Int, name: String, age: Int )
	private case class Event( id: Int, sensor: Int, evType: String, evTs: Timestamp, evValueType: String, evValue: String )
	//	private case class Event( id: Int, sensor: Int )
	def currentTimeStampFunc():java.sql.Timestamp = {
//		var r = scala.util.Random.nextDouble()
//		r
		//			Thread.sleep(   r.nextInt(2) * 1000 )
var ts =		new java.sql.Timestamp( new java.util.Date().getTime() )
ts
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

		//		var foo = spark.range( 1, 1000 )
		//			.select( rand( seed = 10 ).cast( "int" ).as( "c1" ) )

		//		val employee = spark.range( 0, 10 ).
		//			select( $"id".as( "employee_id" ), ( rand() * 3 ).cast( "int" ).as( "dep_id" ), ( rand() * 40 + 20 ).cast( "int" ).as( "age" ) )
		//		val visit = spark.range( 0, 100 ).select( $"id".as( "visit_id" ), when( rand() < 0.95, ( $"id" % 8 ) ).as( "employee_id" ) )
		//		val appointment = spark.range( 0, 100 ).select( $"id".as( "appointment_id" ), when( rand() < 0.95, ( $"id" % 7 ) ).as( "employee_id" ) )
		//		employee.createOrReplaceTempView( "employee" )
		//		visit.createOrReplaceTempView( "visit" )
		//		appointment.createOrReplaceTempView( "appointment" )

		//		employee.show(10)
		//		visit.show(20)
		//		appointment.show(20)

		//		employee.na.drop( "any", Seq( "employee_id", "dep_id", "age" ) )
		//		visit.na.drop( "any", Seq( "visit_id", "employee_id" ) )
		//		appointment.na.drop( "any", Seq( "appointment_id", "employee_id" ) )

		// Register a UDF.
		spark.udf.register( "currentTimeStampFunc", ( arg: Int ) ⇒ {
			var r = scala.util.Random
			//			Thread.sleep(   r.nextInt(2) * 1000 )
			new java.sql.Timestamp( new java.util.Date().getTime() )
		} )
		spark.udf.register( "getEvValueType", ( arg: String ) ⇒ {
			if ( arg.equalsIgnoreCase( "door" ) ) "String" else "Double"
		} )
		spark.udf.register( "getEvValue", ( arg: String ) ⇒ {
			var r = scala.util.Random
			var tempR = scala.util.Random
			if ( arg.equalsIgnoreCase( "door" ) ) { if ( r.nextDouble < 0.5 ) "open" else "close" }
			else {
				String.valueOf( r.nextDouble * 100 )
			}
		} )
		// Example using UDF.
		//		val df = Seq( ( "id1", 1 ), ( "id2", 4 ), ( "id3", 5 ) ).toDF( "id", "value" )
		//		spark.udf.register( "simpleUDF", ( v: Int ) ⇒ v * v )
		//		var foo1 = df.select( $"id", callUDF( "simpleUDF", $"value" ) )
		//		foo1.show()

		// Build a DataFrame, adding column one by one using withColumn.
		// id: Int, sensor: Int, evType: Int, evTime:Timestamp, evValueType:String, evValue:String 
		// withColumn adds additional column, while select creates columns listed in the select statement only.
		// Example:
		//			.select( $"id", $"sensor", callUDF( "currentTimeStampFunc", $"id" ).as( "evTs" ) )// evTs is new column
		//			.withColumn( "evTs", callUDF( "currentTimeStampFunc", $"id" ) ) // id is existing column.

		var evt = spark.range( 1, 10 ) // exclude end of range
		//			.withColumn( "id", $"id".cast( "int" ) )
		//			.withColumn( "sensor", when( rand( 2 ) < 0.5, 123 ).otherwise( 456 ) )
		//			.withColumn( "evType", when( rand( 2 ) < 0.5, "door" ).otherwise( "temp" ) )
		//			.withColumn( "evTs", callUDF( "currentTimeStampFunc", $"id" ) ) // id is existing column.
		//			.withColumn( "evValueType", callUDF( "getEvValueType", $"evType" ) )
		//			.withColumn( "evValue", callUDF( "getEvValue", $"evType" ) )
		//			.as[ Event ]

		//		// To add new columns, use eihter select or withColumn.
		//		val x = emp.select( $"id", $"sensor", callUDF( "currentTimeStampFunc", $"id" ).as( "evTs" ) )
		//		//		val x = emp.withColumn( "evTs", callUDF( "currentTimeStampFunc", $"id" ) ) // evTs is new column; id is existing column.
		//		x.show( 10, false )

//				evt.show( 11, false )

		val seq = Seq( ( 1, "Bob", 23 ), ( 2, "Tom", 23 ), ( 3, "John", 22 ) )
		    seq.foreach{l=> 

//		evt.foreach { l ⇒
			//			println( l.id, l.sensor, l.evType, l.evTs, l.evValueType, l.evValue )
						val eventPath = "hdfs://localhost:8020/user/hduser1/sparkExamples/input/event/event_" + l._1 + ".csv"
			//			var list = List((l.id, l.sensor, l.evType, l.evTs, l.evValueType, l.evValue))
			//			println(list)
			//						println( l.id, l.sensor, l.evType, l.evTs, l.evValueType, l.evValue )
//			val seq2 = Seq( ( 1, 2, 3 ) )
			val ds3 = spark.createDataset( seq )
										.write.format("csv" )
										.option( "sep", "," )
										.option( "path", eventPath)
										.save

			//						spark.createDataset(Seq((l.id, l.sensor, l.evType, l.evTs, l.evValueType, l.evValue)))
			//							.write.format("csv" )
			//							.option( "sep", "," )
			//							.option( "path", eventPath)
			//							.save
		}

		//		//		emp.write.format( "csv" ).option( "sep", "," ).option( "path", eventPath ).save

		//    val seq = Seq((1, "Bob", 23), (2, "Tom", 23), (3, "John", 22))
		//    val ds1 = spark.createDataset(seq)
		//    val ds2: Dataset[(Int, String, Int)] = seq.toDS()
		seq.foreach { l ⇒
			//			val seq2 = Seq((l._1, l._2, l._3))
			val seq2 = Seq( ( 1, 2, 3 ) )
			val ds3 = spark.createDataset( seq2 )
			//			val path = "hdfs://localhost:8020/user/hduser1/sparkExamples/input/event/event_" + l._1 + ".csv"
			//						ds3
			//				.write.format( "csv" )
			//				.option( "sep", "," )
			//				.option( "path", path )
			//				.save()

		}

	}

}