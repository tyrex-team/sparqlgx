#!/bin/bash

PATH_CMD=$(dirname $(dirname $0))
echo "== Generating Statistic Files (Lubm and Watdiv) =="
bash ${PATH_CMD}/bin/generate-stat.sh sparqlgx-test/lubm/init.triples stat-lubm.txt
bash ${PATH_CMD}/bin/generate-stat.sh sparqlgx-test/watdiv/init.triples stat-watdiv.txt
echo ""

exit 0
