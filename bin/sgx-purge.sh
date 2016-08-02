#!/bin/bash

PATH_CMD=$(dirname $0) ;

source ${PATH_CMD}/../conf/sparqlgx.conf

if [[ $# != 1 ]];
then
    echo "Usage: $0 dbName"
    exit 1
fi

dbName=$1

localdbpath=$(sed "s|~|$HOME|g" <<< "$SPARQLGX_LOCAL/$dbName")
hdfsdbpath="$SPARQLGX_HDFS/$dbName/"

rm -rf $localdbpath ;
hadoop fs -rm -r $hdfsdbpath ;

exit 0
