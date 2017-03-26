// https://github.com/yu-iskw/gihyo-spark-book-example/blob/master/src/main/scala/jp/gihyo/spark/ch05/BasicDataFrameExample.scala

package package1
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.sql.Timestamp
import ca.krasnay.sqlbuilder.{ SelectCreator }

import org.springframework.jdbc.core.JdbcTemplate
import java.text.SimpleDateFormat;
import java.sql.{ Date, Timestamp }
import org.apache.spark.sql.Encoders

object dtbExample7 {
	org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope( this )
	case class Station(
		id: Int,
		name: String,
		lat: Double,
		lon: Double,
		dockcount: Int,
		landmark: String,
		installation: String )
	case class Station2(
		id: Int,
		name: String,
		lat: Double,
		lon: Double,
		dockcount: Int,
		landmark: String,
		installation: Date )
	def foo( d: String ): Date = {
		val dateFormat = new SimpleDateFormat( "MM/dd/yyy" )

		val parsedInstallation = dateFormat.parse( d )
		val installation = new java.sql.Date( parsedInstallation.getTime )
		installation

	}
	def main( args: Array[ String ] ) {

		val stationPath = "hdfs://localhost:8020/user/hduser1/sparkExamples/input/201408_station_data.csv"
		val tripPath = "hdfs://localhost:8020/user/hduser1/sparkExamples/input/201408_trip_data.csv"

		val spark = SparkSession
			.builder
			.master( "local[3]" )
			.appName( "BasicDataFrameExample" )
			.getOrCreate()

		val csvSchema = StructType( Array(
			StructField( "id", IntegerType, true ),
			StructField( "name", StringType, true ),
			StructField( "lat", DoubleType, true ),
			StructField( "lon", DoubleType, true ),
			StructField( "dockcount", IntegerType, true ),
			StructField( "landmark", StringType, true ),
			StructField( "installation", StringType, true ) ) )
		import spark.implicits._
		//				implicit val normalPersonKryoEncoder = Encoders.kryo[Station2]
		implicit val e = org.apache.spark.sql.Encoders.DATE // if don't have this one then cannot do map(_.installation) below.
		//		implicit val e = org.apache.spark.sql.Encoders.DATE// Can we use this as encoder and not having to DS->DF->DS, as below?
		val stationDS = spark
			.read
			.option( "sep", "," )
			.option( "header", true )
			.schema( csvSchema ) // Have to specify schema for the csv files
			.csv( stationPath ) // Equivalent to format("csv").load("/path/to/directory") 
			//			.select( $"id", $"name", $"lat", $"lon", $"dockcount", $"landmark", $"installation" )
			//			.as[ Station ]
			.as[ ( Int, String, Double, Double, Int, String, String ) ]

			//			.as[ Station2 ]
			.map( x ⇒ Station2( x._1, x._2, x._3, x._4, x._5, x._6, foo( x._7 ) ) )
		//			.map(_.installation)// need to have org.apache.spark.sql.Encoders.DATE above.
		//		stationDS.show()

		// Change a column type from Int to String and do something with it first before putting the new values into new DS.
		var idString = stationDS.select( col( "id" ).as[ String ] ).map( _ + "test" )
		//		idString.show

		// sort by 
		//		var sortedId = stationDS.sort( 'id ).show()
		
		// OrderBy
//		stationDS.select('id, 'dockcount).orderBy('dockcount.desc).show()
//		stationDS.orderBy('dockcount.desc).show()
//		stationDS.withColumn("le", length('name)).orderBy('le.desc).show()//add new column to use in orderBy

		// Different ways to refer to a column
		//		stationDS.select( 'id, $"name", stationDS( "landmark" ), col( "dockcount" ) ).show()
		//		stationDS.select( 'id, $"name", lit( "STATION" ), expr( "id + 1" ) ).show( 5, false )

		// Select distinct and count
		//		println(stationDS.select('name).distinct().count())
		//		stationDS.select('name).distinct().show(100, false)
		//	println(stationDS.select('landmark).distinct.count)	
		//	stationDS.groupBy('landmark).avg("dockcount").show()
		//	stationDS.groupBy('landmark).sum("dockcount").show
		//	stationDS.groupBy('landmark).count.show
		  stationDS.groupBy('landmark).agg(sum('dockcount), avg('lat), avg('lon)).show
		//	stationDS.filter('landmark === "San Jose").count()
		//	stationDS.select(expr("id + 1").as[Int]).show()

		// Change value of one of the column using a condition when/otherwise.
		// The first gives new DS with all columns and now new column. The second gives new DS with the selected column and new column with new calculated value.
		//		stationDS.withColumn("name", when($"id" === "2", "new name if id is 2").when('id === "3", "new name if id is 3").otherwise('name)).show(100)
		//		stationDS.select('name, when($"id" === "2", "new name if id is 2").when('id === "3", "new name if id is 3").otherwise('name)).show(100)
	
	
	
	
	}
}