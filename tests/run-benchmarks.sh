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
EXPCMD[0]="query"
EXPCMD[1]="query"
EXPCMD[2]="query"
EXPCMD[3]="query"
EXPCMD[4]="query"
EXPCMD[5]="query"
BENCHNAME[0]="lubm";
BENCHNAME[1]="watdiv";
EXPNAME[0]="sgx.nooptim"
EXPNAME[1]="sgx.stat"
EXPNAME[2]="sgx.rf0s"
EXPNAME[3]="sgx.rf100s"
EXPNAME[4]="sgx.rf1000s"
EXPNAME[5]="sgx.rf10000s"
EXPOPT[0]="--no-optim"
EXPOPT[1]="--stat"
EXPOPT[2]="--restricted-stat 0"
EXPOPT[3]="--restricted-stat 100"
EXPOPT[4]="--restricted-stat 1000"
EXPOPT[5]="--restricted-stat 10000"
EXPS="$(seq 0 5)"
NBRUNS=2
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
#    echo -e "| \t|\t     Direct Evaluation\t|\t\t    Standard Evaluation\t\t\t|"
    echo -n "| Query\t|"
    for run in $(seq $NBRUNS) ;
    do
        for exp in $EXPS ;
        do
            printf "%12s\t|" ${EXPNAME[$exp]} ;
        done ;
    done ;
    echo "\n";
    
    for query in ${QUERIES[$b]}; do
        queryfile=$(dirname $0)/resources/queries/$query.rq
        exec 3>&1
        echo -n -e "| $query\t|" 1>&3;
        for run in $(seq $NBRUNS) ;
        do
            for exp in $EXPS ;
            do
                (
                    echo "[$query:${EXPNAME[$exp]} run $run] Start" >> ${logs}.out
                    echo "[$query:${EXPNAME[$exp]} run $run] Start" >> ${logs}.err
                    t1=$(date +%s);
                    if test "${EXPCMD[$exp]}" = "direct-query" ;
                    then
                        endcmd="${DATASET[$b]} $queryfile"
                    else
                        endcmd="${BENCHNAME[$b]} $queryfile"
                    fi ;
                    bash ${PATH_SGX}/sparqlgx.sh ${EXPCMD[$exp]} ${EXPOPT[$exp]} -o $token/results/$query.${EXPNAME[$exp]}.$run.$(date "+%s").txt $endcmd ;
                    t2=$(date +%s);
                    tim=$((t2-t1)) ;
                    echo -n -e "\t$tim\t|" 1>&3;
                    echo "[$query:${EXPNAME[$exp]} run $run] End : $tim" >> ${logs}.out
                    echo "[$query:${EXPNAME[$exp]} run $run] End : $tim" >> ${logs}.err
                ) 2>>${logs}.err | sed -u "s/^/[${query}:${EXPNAME[$exp]}] /" >>${logs}.out ;
            done ;
        done ;
        echo "" ;
        exec 3>&- ;
    done ;
    echo "---------------------------------------------------------------------------------------------------------"
done ;
echo "") | tee -a $tl

stop_time=$(date +%s)

exit 0
