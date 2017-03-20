#!/bin/bash

# Counting given arguments
if [[ $# > 1 ]];
then
    echo "Too many arguments..."
    echo "  To compile: >bash compile.sh"
    echo "  To clean:   >bash compile.sh clean"
    exit 1
fi

# Simple switch to remove compiled files...
case "$1" in
    "clean" )
	echo "Cleaning the project."
	cd $(dirname $0)
	rm -rf bin/sparqlgx-load.jar bin/sparqlgx-stat.jar bin/sparqlgx-translator
	rm -rf src/load-src/project/ src/load-src/target/ src/stat-src/project/ src/stat-src/target/
	cd src/translation-src/
	make clean
	cd ../../
	exit 0
	;;
    
    "" )
	# Skip and compile :)
	;;
    * )
	echo "Usage:"
	echo "  To compile   >bash compile.sh"
	echo "  To clean     >bash compile.sh clean"
	exit 1 
	;;
esac

start_time=$(date +%s)
cd $(dirname $0) # Execution from correct location.
echo "====== SPARQLGX compilation chain ======"
echo "---- Generation of build.sbt ----"
bash bin/generate-build.sh "SPARQLGX Load" > src/load-src/build.sbt
echo "build.sbt in src/load-src/"
bash bin/generate-build.sh "SPARQLGX Statistic Module" > src/stat-src/build.sbt
echo "build.sbt in src/stat-src/"
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
