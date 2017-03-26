// https://github.com/yu-iskw/gihyo-spark-book-example/blob/master/src/main/scala/jp/gihyo/spark/ch05/BasicDataFrameExample.scala
// todo: continue with https://github.com/yu-iskw/gihyo-spark-book-example/blob/master/src/main/scala/jp/gihyo/spark/ch05/BikeShareAnalysisExample.scala
package package1
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.sql.Timestamp
import ca.krasnay.sqlbuilder.{ SelectCreator }
import org.springframework.jdbc.core.JdbcTemplate
import java.text.SimpleDateFormat
import java.sql.{ Date, Timestamp }
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

object dtbExample8 {
	org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope( this )
	case class Station(
		id: Int,
		name: String,
		lat: Double,
		lon: Double,
		dockcount: Int,
		landmark: String,
		installation: Date )
	case class Trip(
		id: Int,
		duration: Int,
		startDate: Timestamp,
		startStation: String,
		startTerminal: Int,
		endDate: Timestamp,
		endStation: String,
		endTerminal: Int,
		bikeNum: Int,
		subscriberType: String,
		zipcode: String )
	def foo( d: String ): Date = {
		val dateFormat = new SimpleDateFormat( "MM/dd/yyy" )
		val parsedInstallation = dateFormat.parse( d )
		val installation = new java.sql.Date( parsedInstallation.getTime )
		installation

	}
	def foo2( d: String ): Timestamp = {
		val dateFormat = new SimpleDateFormat( "MM/dd/yyy HH:mm" )
		val parsedInstallation = dateFormat.parse( d )
		val timestamp = new java.sql.Timestamp( parsedInstallation.getTime )
		timestamp
	}
	def haversineDistance( pointA: ( Double, Double ), pointB: ( Double, Double ) ): Double = {
		val deltaLat = math.toRadians( pointB._1 - pointA._1 )
		val deltaLon = math.toRadians( pointB._2 - pointA._2 )
		val a = math.pow(
			math.sin( deltaLat / 2 ), 2 ) + math.cos( math.toRadians( pointA._1 ) ) *
			math.cos( math.toRadians( pointB._1 ) ) * math.pow( math.sin( deltaLon / 2 ), 2 )
		val greatCircleDistance = 2 * math.atan2( math.sqrt( a ), math.sqrt( 1 - a ) )
		6371000 * greatCircleDistance
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
		implicit val e_DATE = org.apache.spark.sql.Encoders.DATE // if don't have this one then cannot do map(_.installation) below.
		implicit val e_TIMESTAMP = org.apache.spark.sql.Encoders.TIMESTAMP // if don't have this one then cannot do map(_.installation) below.
		val stationDS = spark
			.read
			.option( "sep", "," )
			.option( "header", true )
			.schema( csvSchema ) // Have to specify schema for the csv files
			.csv( stationPath ) // Equivalent to format("csv").load("/path/to/directory") 
			.as[ ( Int, String, Double, Double, Int, String, String ) ]
			.map( x ⇒ Station( x._1, x._2, x._3, x._4, x._5, x._6, foo( x._7 ) ) )
		//			.map(_.installation)// need to have org.apache.spark.sql.Encoders.DATE above.
		stationDS.createOrReplaceTempView( "stationView" )
		//		var test = spark.sql( "select * from stationView where name = \"San Jose Diridon Caltrain Station\"" ).show( true )
		//		stationDS.show()

		// Trip
		//		        val dateFormat = new SimpleDateFormat("MM/dd/yyy HH:mm")
		//
		//        val id = elms(0).toInt
		//        val duration = elms(1).toInt
		//        val startDate = new java.sql.Timestamp(dateFormat.parse(elms(2)).getTime)
		//        val startStation = elms(3)
		//        val startTerminal = elms(4).toInt
		//        val endDate = new java.sql.Timestamp(dateFormat.parse(elms(5)).getTime)
		//        val endStation = elms(6)
		//        val endTerminal = elms(7).toInt
		//        val bikeNum = elms(8).toInt
		//        val subscriberType = elms(9)
		//        val zipcode = elms(10)
		//        Trip(id, duration,
		//          startDate, startStation, startTerminal,
		//          endDate, endStation, endTerminal,
		//          bikeNum, subscriberType, zipcode)

		//Trip ID,Duration,Start Date,Start Station,Start Terminal,End Date,End Station,End Terminal,Bike #,Subscriber Type,Zip Code
		//432946,406,8/31/2014 22:31,Mountain View Caltrain Station,28,8/31/2014 22:38,Castro Street and El Camino Real,32,17,Subscriber,94040
		val csvSchemaTrip = StructType( Array(
			StructField( "id", IntegerType, true ),
			StructField( "duration", IntegerType, true ),
			StructField( "startDate", StringType, true ),
			StructField( "startStation", StringType, true ),
			StructField( "startTerminal", IntegerType, true ),
			StructField( "endDate", StringType, true ),
			StructField( "endStation", StringType, true ),
			StructField( "endTerminal", IntegerType, true ),
			StructField( "bikeNum", IntegerType, true ),
			StructField( "subscriberType", StringType, true ),
			StructField( "zipcode", StringType, true ) ) )

		var tripDS = spark
			.read
			.option( "sep", "," )
			.option( "header", true )
			.schema( csvSchemaTrip )
			.csv( tripPath )
			.as[ ( Int, Int, String, String, Int, String, String, Int, Int, String, String ) ]
			.map( x ⇒ Trip( x._1, x._2, foo2( x._3 ), x._4, x._5, foo2( x._6 ), x._7, x._8, x._9, x._10, x._11 ) )
			.as[ Trip ]

		var tripView = tripDS.createOrReplaceTempView( "tripView" )
		//		spark.sql( """
		//
		//select * from tripView where endStation = "San Jose Diridon Caltrain Station" and 
		//startStation = "San Jose Diridon Caltrain Station" 
		//
		//""" ).show( 45 )
		//		println( spark.sql( """
		//
		//select * from tripView where endStation = "San Jose Diridon Caltrain Station" and 
		//startStation = "San Jose Diridon Caltrain Station" 
		//
		//""" ).count() )

		//		var first = spark.sql( """		
		//		select * from tripView as t join stationView as s
		//		where t.startStation = s.name
		//""" )
		//first.createOrReplaceTempView("first")
		//var second = spark.sql( """
		//		select * from first as f join stationView as s
		//		where f.endStation = s.name
		//""" ).show()

		// Does not work with joinWith, but okay with 'join'.
		var start = stationDS.as( "start" ).toDF()
		var end = stationDS.as( "end" ).toDF()
		start.columns.foreach { col ⇒ start = start.withColumnRenamed( col, s"start_${col}" ) }
		end.columns.foreach { col ⇒ end = end.withColumnRenamed( col, s"end_${col}" ) }

		val joinedTripDF = tripDS.
			join( start, tripDS( "startTerminal" ) === start( "start_id" ), "leftOuter" ).
			join( end, tripDS( "endTerminal" ) === end( "end_id" ), "leftOuter" ).cache
		//		joinedTripDF.show
		//		println( joinedTripDF.count )

			// One way to use udf --> in method select.
		var distance = udf( ( latA: Double, lonA: Double, latB: Double, lonB: Double ) ⇒ haversineDistance( ( latA, latB ), ( lonA, lonB ) ) )
		val tripDistance = joinedTripDF.select( 'id, distance( 'start_lat, 'start_lon, 'end_lat, 'end_lon ) as "dist" )
		spark.udf.register("distance", (latA: Double, lonA: Double, latB: Double, lonB: Double) => haversineDistance( ( latA, latB ), ( lonA, lonB ) ))
		joinedTripDF.createOrReplaceTempView("joinedTrips")
		val tripDistance2 = spark.sql("select distance(start_lat, start_lon, end_lat, end_lon) from joinedTrips").show
//		tripDistance.show()

		tripDS.write.mode( SaveMode.Overwrite ).format( "json" ).save( "hdfs://localhost:8020/user/hduser1/sparkExamples/input/trip.json" )

//		var trip = spark.read.format( "json" ).load( "hdfs://localhost:8020/user/hduser1/sparkExamples/input/trip.json" )
//		trip.createOrReplaceTempView("trips")
//		spark.sql("select * from trips where to_date(startDate) = '2014-08-31'").show()
	
		
		tripDS.unpersist()
		joinedTripDF.unpersist()
		stationDS.unpersist()
	}

}