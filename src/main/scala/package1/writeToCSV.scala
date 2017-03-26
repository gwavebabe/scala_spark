// Create csv entries and write each line to its own file so it can be used in streaming example.
// To use: first start dfs and check resulting csv files in directory hdfs://localhost:8020/user/hduser1/sparkExamples/input/event/.
package package1
import org.apache.spark.sql.{ DataFrame, SparkSession, Column }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
//import org.apache.spark.sql.types.{ StructType, StructField, StringType, LongType, FloatType, DoubleType, DateType, TimestampType, IntegerType }

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
//import org.joda.time.DateTime
//import org.joda.time.format.DateTimeFormat
//import org.joda.time.DateTimeZone
import java.util.Date
object writeToCSV {
	def main( args: Array[ String ] ) {
		val spark = SparkSession
			.builder
			.master( "local[3]" )
			.appName( "myAppName" )
			.getOrCreate()

		import spark.implicits._

		
		var r = scala.util.Random
    val seq = Seq((r.nextInt(100000000), "Bob", 23), (r.nextInt(100000000), "Tom", 23), (r.nextInt(100000000), "John", 22))
    val ds1 = spark.createDataset(seq)
    val ds2: Dataset[(Int, String, Int)] = seq.toDS()
    seq.foreach{l=> 
			val seq2 = Seq((l._1, l._2, l._3))
			val ds3 = spark.createDataset(seq2)
			val path = "hdfs://localhost:8020/user/hduser1/sparkExamples/input/event/event_" + l._1 + ".csv"
						ds3
				.write.format( "csv" )
				.option( "sep", "," )
				.option( "path", path )
				.save()
			
		}
		
	}

}