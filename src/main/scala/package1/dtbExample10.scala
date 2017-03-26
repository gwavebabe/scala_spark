// https://https://github.com/yu-iskw/gihyo-spark-book-example/blob/master/src/main/scala/jp/gihyo/spark/ch05/DatasetExample.scala
package package1
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.sql.Timestamp
import ca.krasnay.sqlbuilder.{ SelectCreator }
import org.springframework.jdbc.core.JdbcTemplate
import java.text.SimpleDateFormat
import java.sql.{ Date, Timestamp }
import org.apache.spark.sql.{ Dataset, Encoders}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrameNaFunctions
object dtbExample10 {
	private case class Person(id: Int, name: String, age: Int)
	def main( args: Array[ String ] ) {
		val spark = SparkSession
			.builder
			.master( "local[3]" )
			.appName( "BasicDataFrameExample" )
			.getOrCreate()

		import spark.implicits._
// continue with Dataset example
    val seq = Seq((1, "Bob", 23), (2, "Tom", 23), (3, "John", 22))
    val ds1 = spark.createDataset(seq)
    val ds2: Dataset[(Int, String, Int)] = seq.toDS()

    // Creates a Dataset from a `RDD`
    val rdd = spark.sparkContext.parallelize(seq)
    val ds3: Dataset[(Int, String, Int)] = spark.createDataset(rdd)
    val ds4: Dataset[(Int, String, Int)] = rdd.toDS()


    val df = rdd.toDF("id", "name", "age")
    val ds5: Dataset[Person] = df.as[Person]

    // Selects a column
//    ds5.select(expr("name").as[String]).show()
//    ds5.select('name).as[String].show
    ds5.map(x => x.id).show
//    ds5.select(_.id).show// using _.id would not work with select

    // Filtering
//    ds5.filter(_.name == "Bob").show()
//    ds5.filter(person => person.age == 23).show()

    // Groups and counts the number of rows
//    ds5.groupBy(_.age).count().show()
	}

}