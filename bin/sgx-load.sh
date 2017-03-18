#!/bin/bash

PATH_CMD=$(dirname $0) ;

source ${PATH_CMD}/../conf/sparqlgx.conf

if [[ $# != 2 ]];
then
    echo "Usage: $0 dbName tripleFile_HDFSPath"
    exit 1
fi

dbName=$1
tripleFile=$2

localdbpath=$(sed "s|~|$HOME|g" <<< "$SPARQLGX_LOCAL/$dbName")
hdfsdbpath="$SPARQLGX_HDFS/$dbName/"

mkdir -p $localdbpath ;
#hadoop fs -mkdir -p $hdfsdbpath ;

spark-submit --driver-memory $SPARK_DRIVER_MEM \
    --executor-memory $SPARK_EXECUT_MEM \
    --class=Load \
    ${PATH_CMD}/sparqlgx-load.jar $tripleFile $hdfsdbpath ;

exit 0
