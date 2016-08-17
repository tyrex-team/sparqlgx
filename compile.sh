#!/bin/bash

start_time=$(date +%s)
cd $(dirname $0) # Execution from correct location.
echo "====== SPARQLGX compilation chain ======"
echo "---- Load Source Compilation ----"
cd src/load-src/
sbt package
cp target/scala*/sparqlgx* ../../bin/sparqlgx-load.jar
cd ../../
echo "---- Stat Source Compilation ----"
cd src/stat-src/
sbt package
cp target/scala*/sparqlgx* ../../bin/sparqlgx-stat.jar
cd ../../
echo "---- Translation Source Compilation ----"
cd src/translation-src/
make
chmod +x sparql2scala
cp sparql2scala ../../bin/sparqlgx-translator
cd ../../
echo "========================================"
echo ""
stop_time=$(date +%s)
echo "The compilation process took $(($stop_time-$start_time)) seconds."
echo ""
echo "Before using SPARQLGX, make sure that:"
echo "  1. The conf file in conf/ is correct"
echo "  2. alias bin/sparqlgx.sh"

exit 0
