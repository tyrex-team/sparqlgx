#!/bin/bash

logs=" /dev/null"
PATH_CMD=$(dirname $0)

source $PATH_CMD/../conf/sparqlgx.conf

nbStep="3"
# HDFS Directories.
echo -e "[1/$nbStep]  HDFS test directories\tcreated at\t$SPARQLGX_HDFS/sparqlgx-test/"
hadoop fs -mkdir -p $SPARQLGX_HDFS/sparqlgx-test/ &> $logs
hadoop fs -mkdir -p $SPARQLGX_HDFS/sparqlgx-test/results/ &> $logs
# Copy N-Triples RDF files.
echo -e "[2/$nbStep]  Dataset 1 (lubm.nt)\tadded in\t$SPARQLGX_HDFS/sparqlgx-test/"
hadoop fs -copyFromLocal ${PATH_CMD}/resources/datasets/lubm.nt $SPARQLGX_HDFS/sparqlgx-test/ &> $logs
echo -e "[3/$nbStep]  Dataset 2 (watdiv.nt)\tadded in\t$SPARQLGX_HDFS/sparqlgx-test/"
hadoop fs -copyFromLocal ${PATH_CMD}/resources/datasets/watdiv.nt $SPARQLGX_HDFS/sparqlgx-test/ &> $logs

exit 0
