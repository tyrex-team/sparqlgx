#!/bin/bash

if [[ $# != 2 ]]
then
    echo "Usage: $0 local/path/query.rq hdfs/path/of/rdf/file"
    exit 1
fi

mkdir -p ./tmp/src/main/scala/
echo -e "name := \"direct-evaluation\"\n\nversion := \"0.1\"\n\nscalaVersion := \"2.10.4\"\n\nlibraryDependencies += \"org.apache.spark\" %% \"spark-core\" % \"1.2.0\"" > ./tmp/build.sbt
echo -e "import org.apache.spark.SparkContext\nimport org.apache.spark.SparkContext._\nimport org.apache.spark.SparkConf\nimport org.apache.spark._\nimport org.apache.spark.rdd.RDD\nobject Query {\ndef main(args: Array[String]) {\nval conf = new SparkConf().setAppName(\"Simple Application\");\nval sc = new SparkContext(conf);\n" > ./tmp/src/main/scala/Query.scala
bin/one_file_translator $2 $1 | sed "s|collect|saveAsTextFile(\"$2-answer.txt\")|g" | sed 's|//|/|g' >> ./tmp/src/main/scala/Query.scala
echo -e "}}" >> ./tmp/src/main/scala/Query.scala
cd tmp/
sbt package
cd ..
spark-submit tmp/target/scala-2.10/direct-evaluation_2.10-0.1.jar
#rm -rf tmp/
exit 0
