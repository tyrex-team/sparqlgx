#!/bin/bash

# This script generates correct build.sbt files which will be needed
# next to compile Scala files. It takes the given arguments to create
# the name of the application. The needed versions of Scala and Spark
# are both written in the file '../conf/compilation.conf'.

PATH_CMD=$(dirname $0)

# Fetching the needed version numbers.
source ${PATH_CMD}/../conf/compilation.conf

# Variable Definitions.
if [[ $# == 0 ]];
then name="Default"
else name="$@"
fi
sparqlgxVersion="$(cat ${PATH_CMD}/../VERSION)"
scalaVersion="$SCALA_VERSION"
sparkVersion="$SPARK_VERSION"

# build.sbt Generation.
echo ""
echo "name := \"$name\""
echo ""
echo "version := \"$sparqlgxVersion\""
echo ""
echo "scalaVersion := \"$scalaVersion\""
echo ""
echo "libraryDependencies += \"org.apache.spark\" %% \"spark-core\" % \"$sparkVersion\""
echo ""

exit 0
