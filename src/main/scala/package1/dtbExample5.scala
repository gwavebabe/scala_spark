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

object dtbExample5 {
	case class event( time: Timestamp, action: String )
	case class Token( name: String, productId: Int, score: Double )

	def main( args: Array[ String ] ) {

		val inputPath = "hdfs://localhost:8020/user/hduser1/sparkExamples/output/databricks_1/"

		val sparkSession = SparkSession
			.builder
			.master( "local[3]" )
			.appName( "time window example" )
			.getOrCreate()
		import sparkSession.implicits._

		val data = Token( "aaa", 100, 0.12 ) ::
			Token( "aaa", 200, 0.29 ) ::
			Token( "bbb", 200, 0.53 ) ::
			Token( "bbb", 300, 0.42 ) :: Nil
		val dataset = data.toDS.cache
		
		dataset.groupByKey(_.name)
	}
}