// https://github.com/yu-iskw/gihyo-spark-book-example/blob/master/src/main/scala/jp/gihyo/spark/ch05/BikeShareAnalysisExample.scalapackage package1
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
import org.apache.spark.sql.DataFrameNaFunctions
object dtbExample11 {
	def main( args: Array[ String ] ) {
		val spark = SparkSession
			.builder
			.master( "local[3]" )
			.appName( "BasicDataFrameExample" )
			.getOrCreate()

		import spark.implicits._

		var testDF = Seq[ ( String, java.lang.Integer, java.lang.Double ) ](
			( "Bob", 16, 176.5 ),
			( "Alice", null, 164.3 ),
			( "", 60, null ),
			( "UNKNOWN", 25, Double.NaN ),
			( "Amy", null, null ),
			( null, null, Double.NaN )
		)
			.toDF( "name", "age", "height" )
		// drop
		//		testDF.na.drop( "any" ).show()
		//		testDF.na.drop( "all" ).show()
		//		testDF.na.drop( Array("age") ).show()
		//		testDF.na.drop( Seq("age", "height") ).show() // if either column has na values, don't include the line
		//		testDF.na.drop( "any",Seq("age", "height") ).show() // exclude line if either column is null
		//		testDF.na.drop( "all",Seq("age", "height") ).show() // exclude line only if both are null
		//		testDF.na.fill( 0.0, Array( "height" ) ).show
		testDF.na.fill( "UNKNOWN" ).show()// replace all String that has 'null' with "UNKNOWN"
		testDF.na.replace( "name", Map( "" -> "UNKNOWN" ) ).show // replace blank string in the column with "UNKNOWN"

		var newDF = testDF.na.fill( Map(
			"age" -> 2,
			"height" -> 0.0,
			"name" -> "UNKNOWN"
		) )
		//		newDF.show

		var myfoo = newDF.map( _.toSeq.toList match {
			case List( a: String, b: Integer, c: Double ) ⇒ "test"
			case _ ⇒ "don't know"
		} )
		//		myfoo.show()

		// Can't do this with match
		//		testDF.map( _ match {
		//			case ( a: String, b: Integer, c: Double ) ⇒ "test"
		//			case _ ⇒ "don't know"
		//		} )

	}

}