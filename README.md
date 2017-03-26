# spark20
Spark 2.0 project with sqlBuilder from ca.krasnay

1. Required HDFS, Spark 2.0.
2. First run genAndWrite2HDFS_DTB_exampleStructured.js from NodeJS_gen project to write files to a HDFS directory.
3. Start dtbExamples4 in this project to read the stream from HDFS and write out to sink.

The project uses a SQL Builder forked from John Krasnay's sqlBuilder https://github.com/gwavebabe/sqlbuilder. It is not necessary to use this builder. Any will do.
