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
	    noOptim="--no-optim"
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
then stat="--stat $localpath/stat.txt";
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
bash ${PATH_CMD}/generate-build.sh "SPARQLGX Evaluation" > $localpath/eval/build.sbt
${PATH_CMD}/sparqlgx-translator $queryFile $noOptim $stat

# Step 2: Compilation.
cd $localpath/eval/
if ! sbt package ;
then
    echo "Compilation failed!" ;
    exit 1 ;
fi ;
cd - > /dev/null

# Step 3: Execution.
spark-submit --driver-memory $SPARK_DRIVER_MEM \
    --executor-memory $SPARK_EXECUT_MEM \
    --class=Query \
    $localpath/eval/target/scala*/evaluation_*.jar "$hdfsdbpath" "$saveFile" ;

# Step 4 [optional]: Cleaning.
if [[ $clean == "1" ]]; then rm -rf $localpath/eval/ ; fi

exit 0
