#!/bin/bash

PATH_CMD=$(dirname $0) ;

source ${PATH_CMD}/../conf/sparqlgx.conf

clean=0
statBool=0
saveFile=""
noOptim=""
sde=""
while true; do
    case "$1" in
        --sde )
            sde="--onefile"
        ;;
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
if [[ -z $sde ]] ;
then
    hdfsdbpath="$SPARQLGX_HDFS/$dbName/"
else
    hdfsdbpath=$dbName ;
fi;

if [[ $statBool == "1" ]] && [[ -f $localpath/stat.txt ]] ;
then stat="--stat $localpath/stat.txt";
else
    [[ $statBool == "1" ]] && (echo "File $localpath/stat.txt not found! Stats deactivated!") ;
    stat="";
fi

########################################################
# Job is done in three main steps:
#  1. The local SPARQL query is translated.
#  2. The translation result is compiled.
#  3. The obtained .jar is executed by spark-submit.
# (4.)Temporary files are removed.
########################################################

# Step 1: Translation.
mkdir -p $localpath/eval/src/main/scala/ ;
bash ${PATH_CMD}/generate-build.sh "SPARQLGX Evaluation" > $localpath/eval/build.sbt
${PATH_CMD}/sparqlgx-translator $queryFile $sde $noOptim $stat > $localpath/eval/src/main/scala/Query.scala

# Step 2: Compilation.
cd $localpath/eval/ ;
rm -f target/scala*/sparqlgx-evaluation_*.jar ;
sync ;
if ! sbt package ;
then
    echo "Compilation failed!" ;
    >&2 echo "Compilation failed!" ;
    if ! sbt package ;
    then
        exit 1 ;
    fi ;
fi ;
cd - > /dev/null

# Step 3: Execution.
spark-submit --driver-memory $SPARK_DRIVER_MEM \
    --executor-memory $SPARK_EXECUT_MEM \
    --class=Query \
    $localpath/eval/target/scala*/sparqlgx-evaluation_*.jar "$hdfsdbpath" "$saveFile" ;

# Step 4 [optional]: Cleaning.
if [[ $clean == "1" ]]; then rm -rf $localpath/eval/ ; fi

exit 0
