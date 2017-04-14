#!/bin/bash

PATH_CMD=$(dirname $0) ;

source ${PATH_CMD}/../conf/sparqlgx.conf

if [[ $# -le 2 ]];
then
    echo "Usage: $0 [light-load|load|stat] dbName tripleFile_HDFSPath"
    exit 1
fi

dbName=$2
tripleFile=$3

localdbpath=$(sed "s|~|$HOME|g" <<< "$SPARQLGX_LOCAL/$dbName")
mkdir -p $localdbpath ;
hdfsdbpath="$SPARQLGX_HDFS/$dbName/"
case "$1" in
    light-load )
        stat=""
        fullstat=""
        load="--load $hdfsdbpath"
        ;;
    load )
        stat="--stat ${localdbpath}/stat.txt"
        fullstat=""
        load="--load $hdfsdbpath"
        ;;
    stat )
        stat="--stat ${localdbpath}/stat.txt"
        fullstat=""
        load=""
        ;;
    all )
        stat="--stat ${localdbpath}/stat.txt"
        fullstat="--full-stat ${localdbpath}/statfull.txt"
        load="--load $hdfsdbpath"
        ;;
    fullstat )
        stat=""
        fullstat="--full-stat ${localdbpath}/statfull.txt"
        load=""
        ;;
esac

spark-submit --driver-memory $SPARK_DRIVER_MEM \
    --executor-memory $SPARK_EXECUT_MEM \
    --class=Main \
    ${PATH_CMD}/sparqlgx-load.jar $tripleFile --stat-size $SPARQLGX_STAT_SIZE $load $stat $fullstat 
