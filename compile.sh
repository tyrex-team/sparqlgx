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
cp target/scala*/sparqlgx* ../../bin/sparqlgx-load-0.1.jar
cd ../../
echo "=== Stat Source Compilation ==="
cd src/stat-src/
sbt package
cd sgrep/
make
cd ../
cp target/scala*/sparqlgx* ../../bin/sparqlgx-stat-0.1.jar
cp sgrep/sgrep ../../bin/sgrep
cd ../../
echo "=== Translation Source Compilation ==="
cd src/translation-src/
make
chmod +x sparql2scala
cp sparql2scala ../../bin/
cd ../../
echo ""

stop_time=$(date +%s)
echo "The compilation process took $(($stop_time-$start_time)) seconds."

exit 0
