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

logs=" /dev/null"
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
echo "'$(bash ${PATH_SGX}sparqlgx.sh -v)' test suite using the following parameters:"
echo -e "\tHDFS path is:\t\t$SPARQLGX_HDFS"
echo -e "\tLocal path is:\t\t$SPARQLGX_LOCAL"
echo -e "\tStatistic size:\t\t$SPARQLGX_STAT_SIZE"
echo -e "\tExecution with:\t\tspark-submit"
echo -e "\t\tDriver   ->\t$SPARK_DRIVER_MEM"
echo -e "\t\tExecutor ->\t$SPARK_EXECUT_MEM"
echo ""
# Query names.
LQ="Q1 Q2 Q3 Q4 Q5 Q6 Q7 Q8 Q9 Q10 Q11 Q12 Q13 Q14"
WQ="C1 C2 C3 F1 F2 F3 F4 F5 L1 L2 L3 L4 L5 S1 S2 S3 S4 S5 S6 S7"
# Time beginning.
start_time=$(date +%s)
# Loads.
echo "--------------------- LOAD ----------------------"
t1_beg=$(date +%s); bash ${PATH_SGX}/sparqlgx.sh light-load lubm $SPARQLGX_HDFS/sparqlgx-test/lubm.nt &>> $logs ; t1_end=$(date +%s);
t2_beg=$(date +%s); bash ${PATH_SGX}/sparqlgx.sh generate-stat lubm $SPARQLGX_HDFS/sparqlgx-test/lubm.nt &>> $logs ; t2_end=$(date +%s);
echo "> LUBM dataset loaded in $(($t1_end-$t1_beg))s and its statistics generated in $((t2_end-$t2_beg))s."
t1_beg=$(date +%s); bash ${PATH_SGX}/sparqlgx.sh light-load watdiv $SPARQLGX_HDFS/sparqlgx-test/watdiv.nt &>> $logs ; t1_end=$(date +%s);
t2_beg=$(date +%s); bash ${PATH_SGX}/sparqlgx.sh generate-stat watdiv $SPARQLGX_HDFS/sparqlgx-test/watdiv.nt &>> $logs ; t2_end=$(date +%s);
echo "> WATDIV dataset loaded in $(($t1_end-$t1_beg))s and its statistics generated in $((t2_end-$t2_beg))s."
echo ""
# LUBM.
echo "----------------------------------------------- EVAL LUBM -----------------------------------------------"
echo -e "| \t|\t     Direct Evaluation\t\t|\t\t    Standard Evaluation\t\t\t|"
echo -e "| Query\t|\tNo Optim\tStandard\t|\tNo Optim\tStandard\tWith Statistics\t|"
for i in $LQ; do
    # The same query is done 5 times: depending on optimizations
    # sketches (with or without any, statistics or not...).
    t1_beg=$(date +%s); bash ${PATH_SGX}/sparqlgx.sh direct-query --no-optim -o $SPARQLGX_HDFS/sparqlgx-test/results/$i.sde.nooptim.txt $(dirname $0)/resources/queries/$i.rq $SPARQLGX_HDFS/sparqlgx-test/lubm.nt &>> $logs ; t1_end=$(date +%s);
    t2_beg=$(date +%s); bash ${PATH_SGX}/sparqlgx.sh direct-query -o $SPARQLGX_HDFS/sparqlgx-test/results/$i.sde.txt $(dirname $0)/resources/queries/$i.rq $SPARQLGX_HDFS/sparqlgx-test/lubm.nt &>> $logs ; t2_end=$(date +%s);
    t3_beg=$(date +%s); bash ${PATH_SGX}/sparqlgx.sh query --no-optim -o $SPARQLGX_HDFS/sparqlgx-test/results/$i.sgx.nooptim.txt lubm $(dirname $0)/resources/queries/$i.rq &>> $logs ; t3_end=$(date +%s);
    t4_beg=$(date +%s); bash ${PATH_SGX}/sparqlgx.sh query -o $SPARQLGX_HDFS/sparqlgx-test/results/$i.sgx.txt lubm $(dirname $0)/resources/queries/$i.rq &>> $logs ; t4_end=$(date +%s);
    t5_beg=$(date +%s); bash ${PATH_SGX}/sparqlgx.sh query --stat -o $SPARQLGX_HDFS/sparqlgx-test/results/$i.sgx.stat.txt lubm $(dirname $0)/resources/queries/$i.rq &>> $logs ; t5_end=$(date +%s);
    echo -e "| $i\t|\t$((t1_end-t1_beg))\t\t$((t2_end-t2_beg))\t\t|\t$((t3_end-t3_beg))\t\t$((t4_end-t4_beg))\t\t$((t5_end-t5_beg))\t\t|"
done
echo "---------------------------------------------------------------------------------------------------------"
echo ""
# WatDiv.
echo "---------------------------------------------- EVAL WATDIV ----------------------------------------------"
echo -e "| \t|\t     Direct Evaluation\t\t|\t\t    Standard Evaluation\t\t\t|"
echo -e "| Query\t|\tNo Optim\tStandard\t|\tNo Optim\tStandard\tWith Statistics\t|"
for i in $WQ; do
    # The same query is done 5 times: depending on optimizations
    # sketches (with or without any, statistics or not...).
    t1_beg=$(date +%s); bash ${PATH_SGX}/sparqlgx.sh direct-query --no-optim -o $SPARQLGX_HDFS/sparqlgx-test/results/$i.sde.nooptim.txt $(dirname $0)/resources/queries/$i.rq $SPARQLGX_HDFS/sparqlgx-test/watdiv.nt &>> $logs ; t1_end=$(date +%s);
    t2_beg=$(date +%s); bash ${PATH_SGX}/sparqlgx.sh direct-query -o $SPARQLGX_HDFS/sparqlgx-test/results/$i.sde.txt $(dirname $0)/resources/queries/$i.rq $SPARQLGX_HDFS/sparqlgx-test/watdiv.nt &>> $logs ; t2_end=$(date +%s);
    t3_beg=$(date +%s); bash ${PATH_SGX}/sparqlgx.sh query --no-optim -o $SPARQLGX_HDFS/sparqlgx-test/results/$i.sgx.nooptim.txt watdiv $(dirname $0)/resources/queries/$i.rq &>> $logs ; t3_end=$(date +%s);
    t4_beg=$(date +%s); bash ${PATH_SGX}/sparqlgx.sh query -o $SPARQLGX_HDFS/sparqlgx-test/results/$i.sgx.txt watdiv $(dirname $0)/resources/queries/$i.rq &>> $logs ; t4_end=$(date +%s);
    t5_beg=$(date +%s); bash ${PATH_SGX}/sparqlgx.sh query --stat -o $SPARQLGX_HDFS/sparqlgx-test/results/$i.sgx.stat.txt watdiv $(dirname $0)/resources/queries/$i.rq &>> $logs ; t5_end=$(date +%s);
    echo -e "| $i\t|\t$((t1_end-t1_beg))\t\t$((t2_end-t2_beg))\t\t|\t$((t3_end-t3_beg))\t\t$((t4_end-t4_beg))\t\t$((t5_end-t5_beg))\t\t|"
done
echo "---------------------------------------------------------------------------------------------------------"
echo ""
# End.
stop_time=$(date +%s)

exit 0
