#!/bin/bash

PATH_CMD=$(dirname $0) ;

source ${PATH_CMD}/../conf/sparqlgx.conf

clean=0
statBool=0
saveFile=""
noOptim=""
while true; do
    case "$1" in
	--clean )
	    clean=1
	    shift
	    ;;
	--stat )
	    statBool=1
	    shift
	    ;;
	--no-optim )
	    noOptim="-no-optim"
	    shift
	    ;;
	-o )
	    saveFile=$2
	    shift 2
	    ;;
	*)
	    break
	    ;;
    esac
done


if [[ $# != 2 ]];
then
    echo "Usage: $0 [-o responseFile_HDFSPath] [--no-optim] [--stat] [--clean] dbName queryFile_LocalPath"
    exit 1
fi
dbName=$1
queryFile=$2
localpath=$(sed "s|~|$HOME|g" <<< "$SPARQLGX_LOCAL/$dbName")
hdfsdbpath="$SPARQLGX_HDFS/$dbName/"
if [[ $statBool == "1" ]] && [[ ! -f $localpath/stat.txt ]];
then stat="-stat $localpath/stat.txt";
else stat="";
fi

########################################################
# Job is done in three main steps:
#  1. The local SPARQL query is translated.
#  2. The translation result is compiled.
#  3. The obtained .jar is executed by spark-submit.
# (4.)Temporary files are removed.
########################################################

# Step 1: Translation.
if [[ ! -d $localpath/eval/src/main/scala ]];
then mkdir -p $localpath/eval/src/main/scala/ ;
fi
echo -e "name := \"evaluation\"\n\nversion := \"0.1\"\n\nscalaVersion := \"2.11.0\"\n\nlibraryDependencies += \"org.apache.spark\" %% \"spark-core\" % \"2.1.0\"" > $localpath/eval/build.sbt
echo -e "import org.apache.spark.SparkContext\nimport org.apache.spark.SparkContext._\nimport org.apache.spark.SparkConf\nimport org.apache.spark._\nimport org.apache.spark.rdd.RDD\nimport org.apache.log4j.Logger\nimport org.apache.log4j.Level\nobject Query {\ndef main(args: Array[String]) {\nLogger.getLogger(\"org\").setLevel(Level.OFF);\nLogger.getLogger(\"akka\").setLevel(Level.OFF);\nval conf = new SparkConf().setAppName(\"Simple Application\");\nval sc = new SparkContext(conf);\n" > $localpath/eval/src/main/scala/Query.scala
if [[ -z $saveFile ]];
then
    ${PATH_CMD}/sparqlgx-translator $queryFile $noOptim $stat | sed "s_\"DATAHDFSPATH\"_\"$hdfsdbpath\"_" | sed "s|collect|collect().foreach(println)|g" >> $localpath/eval/src/main/scala/Query.scala
else 
    ${PATH_CMD}/sparqlgx-translator $queryFile $noOptim $stat | sed "s_\"DATAHDFSPATH\"_\"$hdfsdbpath\"_" | sed "s|collect|saveAsTextFile(\"$saveFile\")|g" >> $localpath/eval/src/main/scala/Query.scala
fi
echo -e "}}" >> $localpath/eval/src/main/scala/Query.scala

# Step 2: Compilation.
cd $localpath/eval/
sbt package
cd - > /dev/null

# Step 3: Execution.
spark-submit --driver-memory $SPARK_DRIVER_MEM \
    --executor-memory $SPARK_EXECUT_MEM \
    --class=Query \
    $localpath/eval/target/scala-2.10/evaluation_2.10-0.1.jar ;

# Step 4 [optional]: Cleaning.
if [[ $clean == "1" ]]; then rm -rf $localpath/eval/ ; fi

exit 0
