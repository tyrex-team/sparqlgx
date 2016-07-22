#!/bin/bash

PATH_CMD=$(dirname $(dirname $0))
spark-submit --class "Load" ${PATH_CMD}/bin/sparqlgx-load-0.1.jar sparqlgx-test/lubm/init.triples sparqlgx-test/lubm/
sleep 10
bash ${PATH_CMD}/bin/sparqlgx-eval.sh ${PATH_CMD}/tests/lubm/Q1.rq sparqlgx-test/lubm/

exit 0
