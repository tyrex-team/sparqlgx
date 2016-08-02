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

spark-submit --driver-memory $SPARK_DRIVER_MEM \
    --executor-memory $SPARK_EXECUT_MEM \
    --class=GenerateStat \
    ${PATH_CMD}/sparqlgx-stat.jar $tripleFile $SPARQLGX_STAT_SIZE \
    > $localdbpath/stat.txt ;

exit 0
