#!/bin/bash
###########################################################################
## A simple script that should be launched when starting the sparqlgx    ##
## session after building the Dockerfile to give users some instruction. ##
###########################################################################
# Defining some colors in terminal.
RED='\033[0;31m'
GREEN='\033[0;32m'
BLACK='\033[0;33m'
BLUE='\033[0;34m'
CODE='\033[0;104m'
NC='\033[0m'
# Printing the welcoming message.
echo -e "/---------------------------------------------------------------------------\\"
echo -e "|                      ${RED}=== SPARQLGX Demonstrator ===${NC}"
echo -e "|"
echo -e "| ${GREEN}Overview:${NC}"
echo -e "|   The idea of this Docker image is to make reproducible simple usages\n|   of SPARQLGX and SDE (its direct evaluator: without loading phase)."
echo -e "|"
echo -e "| ${GREEN}What to do?${NC}"
echo -e "|   This image contains a ready to-be-used instance of SPARQLGX on top of\n|   Apache Spark and Hadoop."
echo -e "|   Basically, one can:"
echo -e "|      1. Explore the various features provided by SPARQLGX such as the\n|      possible loading scenarios, and then query data using for instance\n|      statistics obtained during the loading phase."
echo -e "|      To know more about SPARQLGX syntax:"
echo -e "|          > ${CODE}sparqlgx --help${NC}"
echo -e "|      2. Run SPARQLGX using various famous benchmarks i.e. WatDiv & LUBM\n|      following these instructions:"
echo -e "|          > ${CODE}cd tests/${NC}"
echo -e "|          > ${CODE}bash preliminaries.sh${NC}"
echo -e "|          > ${CODE}bash run-benchmarks.sh${NC}"
echo -e "|          > ${CODE}bash clean-all.sh${NC}"
echo -e "|"
echo -e "| ${GREEN}Credits:${NC}"
echo -e "|   Damien Graux, <${BLUE}https://dgraux.github.io${NC}>"
echo -e "|   2019"
echo -e "\\---------------------------------------------------------------------------/"
