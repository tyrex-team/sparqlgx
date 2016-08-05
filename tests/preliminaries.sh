#!/bin/bash

logs=" /dev/null"
PATH_CMD=$(dirname $0)

nbStep="3"
# HDFS Directories.
echo -e "[1/$nbStep]  HDFS test directories\tcreated at\tsparqlgx-test/"
hadoop fs -mkdir sparqlgx-test/ &> $logs
hadoop fs -mkdir sparqlgx-test/results/ &> $logs
# Copy N-Triples RDF files.
echo -e "[2/$nbStep]  Dataset 1 (lubm.nt)\tadded in\tsparqlgx-test/"
hadoop fs -copyFromLocal ${PATH_CMD}/resources/datasets/lubm.nt sparqlgx-test/ &> $logs
echo -e "[3/$nbStep]  Dataset 2 (watdiv.nt)\tadded in\tsparqlgx-test/"
hadoop fs -copyFromLocal ${PATH_CMD}/resources/datasets/watdiv.nt sparqlgx-test/ &> $logs

exit 0
