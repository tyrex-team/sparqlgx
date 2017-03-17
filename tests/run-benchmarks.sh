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
BENCHNAME[0]="lubm";
BENCHNAME[1]="watdiv";
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
    t1=$(date +%s);
    bash ${PATH_SGX}/sparqlgx.sh light-load ${BENCHNAME[$b]} ${DATASET[$b]} 1>>${logs}.out 2>>${logs}.err ;
    t2=$(date +%s);
    bash ${PATH_SGX}/sparqlgx.sh generate-stat ${BENCHNAME[$b]} ${DATASET[$b]} 1>>${logs}.out 2>>${logs}.err ;
    t3=$(date +%s);
    echo "> ${BENCHNAME[$b]} dataset loaded in $((t2-t1))s and its statistics generated in $((t3-t2))s."
    echo "----------------------------------------------- EVAL ----------------------------------------------------"
    echo -e "| \t|\t     Direct Evaluation\t\t|\t\t    Standard Evaluation\t\t\t|"
    echo -e "| Query\t|\tNo Optim\tStandard\t|\tNo Optim\tStandard\tWith Statistics\t|"
    for i in ${QUERIES[$b]}; do
        # The same query is done 5 times: depending on optimizations
        # sketches (with or without any, statistics or not...).
        exec 3>&1
        (
            echo -n -e "| $i\t|" 1>&3;
            t1=$(date +%s);
            bash ${PATH_SGX}/sparqlgx.sh direct-query --no-optim -o $token/results/$i.sde.nooptim.txt $(dirname $0)/resources/queries/$i.rq ${DATASET[$b]} ;
            t2=$(date +%s);
            echo -n -e "\t$((t2-t1))" 1>&3;
            bash ${PATH_SGX}/sparqlgx.sh direct-query -o $token/results/$i.sde.txt $(dirname $0)/resources/queries/$i.rq ${DATASET[$b]}  ;
            t3=$(date +%s);
            echo -n -e "\t\t$((t3-t2))" 1>&3;
            bash ${PATH_SGX}/sparqlgx.sh query --no-optim -o $token/results/$i.sgx.nooptim.txt ${BENCHNAME[$b]} $(dirname $0)/resources/queries/$i.rq ;
            t4=$(date +%s);
            echo -n -e "\t\t|\t$((t4-t3))" 1>&3;
            bash ${PATH_SGX}/sparqlgx.sh query -o $token/results/$i.sgx.txt ${BENCHNAME[$b]} $(dirname $0)/resources/queries/$i.rq  ;
            t5=$(date +%s);
            echo -n -e "\t\t$((t5-t4))" 1>&3;
            bash ${PATH_SGX}/sparqlgx.sh query --stat -o $token/results/$i.sgx.stat.txt ${BENCHNAME[$b]} $(dirname $0)/resources/queries/$i.rq  ;
            t6=$(date +%s);
            echo -e "\t\t$((t6-t5))\t\t|" 1>&3;
        ) 2>>${logs}.err | sed -u "s/^/[$i] /" >>${logs}.out ;
        exec 3>&- ;
    done
    echo "---------------------------------------------------------------------------------------------------------"
done ;
echo "") | tee -a $tl

stop_time=$(date +%s)

exit 0
