#!/bin/bash

spark-submit --class "Load" ../bin/sparqlgx-load-0.1.jar sparqlgx-test/watdiv/init.triples sparqlgx-test/watdiv/
sleep 10
bash ../bin/sparqlgx-eval.sh watdiv/C1.rq sparqlgx-test/watdiv/

exit 0
