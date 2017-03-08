#!/bin/bash

PATH_CMD=$(dirname $0)/../bin/

nbStep="2"
# Cleaning HDFS resources and results.
echo -e "[1/$nbStep]  HDFS test directory\tremoved"
hadoop fs -rm -r $SPARQLGX_HDFS/sparqlgx-test &> /dev/null
# Purging databases.
echo -e "[2/$nbStep]  Databases\t\tremoved"
bash ${PATH_CMD}/sparqlgx.sh remove lubm &> /dev/null
bash ${PATH_CMD}/sparqlgx.sh remove watdiv &> /dev/null

exit 0
