#!/bin/bash

######################### == Benchmark Tests == #######################
##                                                                   ##
## 1. Complete loads are done for each datasets.                     ##
##                                                                   ##
## 2. Direct evaluations without optimisations are realized and then ##
## direct evaluations avoiding cartesian products.                   ##
##                                                                   ##
## 3. Conventional evaluations (using the preprocessed verticaly     ##
## partitioned files) are done three times: (a) without any          ##
## optimizations; (b) avoiding cartesian products; (c) avoiding      ##
## cartesian products and taking into account statistics on data.    ##
##                                                                   ##
#######################################################################

PATH_SGX=$(dirname $0)/../bin/
source ${PATH_SGX}/../conf/sparqlgx.conf

# Check that 'preliminaries.sh' has been run.
if hadoop fs -test -e $SPARQLGX_HDFS/sparqlgx-test/lubm.nt && hadoop fs -test -e $SPARQLGX_HDFS/sparqlgx-test/watdiv.nt ;
then :
else
    echo "[FAIL] -- You must run 'preliminaries.sh' before."
    exit 1
fi

# Brief remind of parameters.
# Query names.
QUERIES[0]="Q1 Q2 Q3 Q4 Q5 Q6 Q7 Q8 Q9 Q10 Q11 Q12 Q13 Q14";
QUERIES[1]="C1 C2 C3 F1 F2 F3 F4 F5 L1 L2 L3 L4 L5 S1 S2 S3 S4 S5 S6 S7";
DATASET[0]=$SPARQLGX_HDFS/sparqlgx-test/lubm.nt ;
DATASET[1]=$SPARQLGX_HDFS/sparqlgx-test/watdiv.nt ;
EXPCMD[0]="direct-query"
EXPCMD[1]="direct-query"
EXPCMD[2]="query"
EXPCMD[3]="query"
EXPCMD[4]="query"
BENCHNAME[0]="lubm";
BENCHNAME[1]="watdiv";
EXPNAME[0]="sde.nooptim"
EXPNAME[1]="sde.normal"
EXPNAME[2]="sgx.nooptim"
EXPNAME[3]="sgx.normal"
EXPNAME[4]="sgx.stats"
EXPOPT[0]="--no-optim"
EXPOPT[1]=""
EXPOPT[2]="--no-optim"
EXPOPT[3]=""
EXPOPT[4]="--stat"

# Time beginning.
start_time=$(date +%s)
# Loads.
tokname=$(cat ../.git/refs/heads/master)__$(date "+%Y-%m-%d_%H-%M-%S") ;
token="$SPARQLGX_HDFS/$tokname"
logs="logs.$tokname"                                                                                    
tl="time.$tokname"                                                                                   

ln -sf ${logs}.out out.latest
ln -sf ${logs}.err err.latest

(echo "'$(bash ${PATH_SGX}sparqlgx.sh -v)' test suite using the following parameters:"
echo -e "\tHDFS path is:\t\t$SPARQLGX_HDFS"
echo -e "\tLocal path is:\t\t$SPARQLGX_LOCAL"
echo -e "\tStatistic size:\t\t$SPARQLGX_STAT_SIZE"
echo -e "\tExecution with:\t\tspark-submit"
echo -e "\t\tDriver   ->\t$SPARK_DRIVER_MEM"
echo -e "\t\tExecutor ->\t$SPARK_EXECUT_MEM"
echo -e "\t\tToken: $tokname"
echo -e "\t\tResults: $token"
echo -e "\t\tLogs: $logs"
echo "" ;
hadoop fs -mkdir -p $token/results ;
for b in 0 1 ;
do
    echo "";
    echo "";
    echo "";
    echo "---------------------"
    echo "         ${BENCHNAME[$b]}       "
    echo "---------------------"
    echo "";
    echo "--------------------- LOAD ----------------------"
    echo "Start loading" >>${logs}.err
    echo "Start loading" >>${logs}.out
    t1=$(date +%s);
    bash ${PATH_SGX}/sparqlgx.sh load ${BENCHNAME[$b]} ${DATASET[$b]} 1>>${logs}.out 2>>${logs}.err ;
    t2=$(date +%s);
    echo "Finished loading" >>${logs}.err
    echo "Finished loading" >>${logs}.out
    echo "> ${BENCHNAME[$b]} dataset loaded in $((t2-t1))s."
    echo "----------------------------------------------- EVAL ----------------------------------------------------"
    echo -e "| \t|\t     Direct Evaluation\t|\t\t    Standard Evaluation\t\t\t|"
    echo -e "| Query\t|  No Optim\t|  Standard\t|  No Optim\t|  Standard\t| With Stats\t|"
    for query in ${QUERIES[$b]}; do
        # The same query is done 5 times: depending on optimizations
        # sketches (with or without any, statistics or not...).
        queryfile=$(dirname $0)/resources/queries/$query.rq
        exec 3>&1
        echo -n -e "| $query\t|" 1>&3;
        for exp in $(seq 0 4) ;
        do
                (
                    echo "[$query:${EXPNAME[$exp]}] Start" >> ${logs}.out
                    echo "[$query:${EXPNAME[$exp]}] Start" >> ${logs}.err
                    t1=$(date +%s);
                    if test "${EXPCMD[$exp]}" = "direct-query" ;
                    then
                        endcmd="${DATASET[$b]} $queryfile"
                    else
                        endcmd="${BENCHNAME[$b]} $queryfile"
                    fi ;
                    bash ${PATH_SGX}/sparqlgx.sh ${EXPCMD[$exp]} ${EXPOPT[$exp]} -o $token/results/$query.${EXPNAME[$exp]}.txt $endcmd ;
                    t2=$(date +%s);
                    tim=$((t2-t1)) ;
                    echo -n -e "\t$tim\t|" 1>&3;
                    echo "[$query:${EXPNAME[$exp]}] End : $tim" >> ${logs}.out
                    echo "[$query:${EXPNAME[$exp]}] End : $tim" >> ${logs}.err
                ) 2>>${logs}.err | sed -u "s/^/[${query}:${EXPNAME[$exp]}] /" >>${logs}.out ;
        done ;
        echo "" ;
        exec 3>&- ;
    done ;
    echo "---------------------------------------------------------------------------------------------------------"
done ;
echo "") | tee -a $tl

stop_time=$(date +%s)

exit 0
