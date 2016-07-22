#!/bin/bash

PATH_CMD=$(dirname $0) ;

if [[ $# != 2 ]];
then
    echo "Usage $0 tripleFilePath statisticOutputOnLocalDisk"
    exit 1
fi

spark-submit --class=GenerateStat ${PATH_CMD}/sparqlgx-stat_2.10-0.1.jar $1
hadoop fs -cat sparqlgxstatistics.txt/* > $2
hadoop fs -rm -r sparqlgxstatistics.txt

exit 0
