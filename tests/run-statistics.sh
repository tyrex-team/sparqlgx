#!/bin/bash

echo "== Generating Statistic Files (Lubm and Watdiv) =="
bash ../bin/generate-stat.sh sparqlgx-test/lubm/init.triples stat-lubm.txt
bash ../bin/generate-stat.sh sparqlgx-test/watdiv/init.triples stat-watdiv.txt
echo ""

exit 0
