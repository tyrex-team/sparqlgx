#!/bin/bash
start_time=$(date +%s)

#if [[ ! -d "bin/" ]];
#then mkdir bin/ ;
#else rm bin/* ;
#fi

echo "====== SPARQLGX compilation chain ======"
echo "=== Load Source Compilation ==="
cd src/load-src/
sbt package
cp target/scala*/sparqlgx* ../../bin/sparqlgx-load.jar
cd ../../
echo "=== Stat Source Compilation ==="
cd src/stat-src/
sbt package
cd sgrep/
make
cd ../
cp target/scala*/sparqlgx* ../../bin/sparqlgx-stat.jar
cp sgrep/sgrep ../../bin/sgrep
cd ../../
echo "=== Translation Source Compilation ==="
cd src/translation-src/
make
chmod +x sparql2scala
cp sparql2scala ../../bin/sparqlgx-translator
cd ../../
echo ""

echo "----------------------------------------"
echo "Before using SPARQLGX, make sure that:"
echo "  1. The conf file in conf/ is correct"
echo "  2. alias bin/sparqlgx.sh"
echo "----------------------------------------"

stop_time=$(date +%s)
echo "The compilation process took $(($stop_time-$start_time)) seconds."

exit 0
