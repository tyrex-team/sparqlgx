#!/bin/bash

spark-submit --class "Load" ../bin/sparqlgx-load-0.1.jar sparqlgx-test/lubm/init.triples sparqlgx-test/lubm/
sleep 10
bash ../bin/sparqlgx-eval.sh lubm/Q1.rq sparqlgx-test/lubm/

exit 0
