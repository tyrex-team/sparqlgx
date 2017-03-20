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
hdfsdbpath="$SPARQLGX_HDFS/$dbName/"
case "$1" in
    light-load )
        statfile="/dev/null"
        opt="--no-stat"
        ;;
    load )
        statfile="${localdbpath}/stat.txt" ;
        mkdir -p $localdbpath ;
        opt=""
        ;;
    stat )
        opt="--no-load" ;
        statfile="${localdbpath}/stat.txt" ;
        mkdir -p $localdbpath
        ;;
esac

spark-submit --driver-memory $SPARK_DRIVER_MEM \
    --executor-memory $SPARK_EXECUT_MEM \
    --class=Main \
    ${PATH_CMD}/sparqlgx-load.jar $tripleFile --hdfs-path $hdfsdbpath --stat-size $SPARQLGX_STAT_SIZE $opt  > $statfile
