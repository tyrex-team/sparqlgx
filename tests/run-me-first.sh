#!/bin/bash

# Directories
hadoop fs -mkdir sparqlgx-test/
hadoop fs -mkdir sparqlgx-test/lubm/
hadoop fs -mkdir sparqlgx-test/watdiv/
# N-Triple RDF files
hadoop fs -copyFromLocal lubm/lubm.nt sparqlgx-test/lubm/init.triples
hadoop fs -copyFromLocal watdiv/watdiv.nt sparqlgx-test/watdiv/init.triples

exit 0
