name := "SparkTwoExperiments"

version := "1.0"

//scalaVersion := "2.10.4"
scalaVersion := "2.11.0"

//val sparkVersion = "2.0.0-SNAPSHOT"
val sparkVersion = "2.0.0"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion
)
// https://mvnrepository.com/artifact/net.liftweb/lift-json_2.10
libraryDependencies += "net.liftweb" % "lift-json_2.10" % "2.6.3"


// https://mvnrepository.com/artifact/com.github.nscala-time/nscala-time_2.10
libraryDependencies += "com.github.nscala-time" % "nscala-time_2.10" % "2.12.0"

// https://mvnrepository.com/artifact/com.databricks/spark-csv_2.10
//libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.5.0"


// https://mvnrepository.com/artifact/joda-time/joda-time
libraryDependencies += "joda-time" % "joda-time" % "2.9.4"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.10
//libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "2.0.0"

// https://mvnrepository.com/artifact/com.googlecode.json-simple/json-simple
libraryDependencies += "com.googlecode.json-simple" % "json-simple" % "1.1.1"

// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk
//libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.43"
//libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.9.40"
//libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.8.9"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.9.37"
// https://mvnrepository.com/artifact/com.amazonaws/amazon-kinesis-client
//libraryDependencies += "com.amazonaws" % "amazon-kinesis-client" % "1.7.0"
libraryDependencies += "com.amazonaws" % "amazon-kinesis-client" % "1.5.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kinesis-asl_2.11
libraryDependencies += "org.apache.spark" % "spark-streaming-kinesis-asl_2.11" % "2.0.1"


// sql builder from https://github.com/jkrasnay/sqlbuilder
libraryDependencies += "ca.krasnay" % "sqlbuilder" % "1.2"



