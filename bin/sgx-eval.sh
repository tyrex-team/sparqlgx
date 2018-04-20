#!/bin/bash

PATH_CMD=$(dirname $0) ;

source ${PATH_CMD}/../conf/sparqlgx.conf

clean=0
stat=0
saveFile=""
noOptim=""
sde=""
while true; do
    case "$1" in
        --sde )
            sde="--onefile"
            shift
        ;;
	--clean )
	    clean=1
	    shift
	    ;;
	--stat )
	    stat=1
	    shift
	    ;;
	--restricted-stat )
	    stat=3
	    nb_stats=$2
            shift 2
	    ;;

	--fullstat )
	    stat=2
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
localpath=$(sed "s|~|$HOME|g" <<< "$SPARQLGX_LOCAL/")
if [[ -z $sde ]] ;
then
    hdfsdbpath="$SPARQLGX_HDFS/$dbName/"
else
    hdfsdbpath=$dbName ;
fi;

if [[ $stat == "1" ]] && [[ -f $localpath/$dbName/stat.txt ]] ;
then stat="--stat $localpath/$dbName/stat.txt";
else
    if [[ $stat == "2" ]] && [[ -f $localpath/$dbName/statfull.txt ]] ;
    then stat="--fullstat $localpath/$dbName/statfull.txt";
    else
        if [[ $stat == "3" ]] && [[ -f $localpath/$dbName/statfull.txt ]] ;
        then stat="--restricted-stat $nb_stats $localpath/$dbName/statfull.txt";
        else
            [[ $stat != "0" ]] && (echo "File stat in $localpath not found! Stats deactivated!") ;
            stat="";
        fi ;
    fi;
fi;

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
${PATH_CMD}/sparqlgx-translator $queryFile  --prefix ${localpath}/${dbName}/prefix.txt $sde $noOptim $stat > $localpath/eval/src/main/scala/Query.scala

# Step 2: Compilation.
cd $localpath/eval/ ;
rm -f target/scala*/sparqlgx-evaluation_*.jar ;
sync ;
if ! sbt -verbose -batch clean package ;
then
    echo "Compilation failed!" ;
    >&2 echo "Compilation failed!" ;
    if ! sbt  -verbose -batch clean package ;
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
