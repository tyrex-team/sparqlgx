#!/bin/bash

PATH_CMD=$(dirname $0) ;

source ${PATH_CMD}/../conf/sparqlgx.conf

statBool=0
noOptim=""
while true; do
    case "$1" in
	--stat )
	    statBool=1
	    shift
	    ;;
	--no-optim )
	    noOptim="--no-optim"
	    shift
	    ;;
	*)
	    break
	    ;;
    esac
done


if [[ $# != 2 ]];
then
    echo "Usage: $0 [--no-optim] [--stat] dbName queryFile_LocalPath"
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

${PATH_CMD}/sparqlgx-translator $queryFile $noOptim $stat "--debug" | sed "s|\"DATAHDFSPATH\"|\"$hdfsdbpath\"|" | sed "s|collect|collect().foreach(println)|g"

exit 0
