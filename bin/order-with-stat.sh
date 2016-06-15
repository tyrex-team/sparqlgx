#!/bin/bash


##############################
##############################
# isVar uri
function isVar {
    if [[ ${1:0:1} == "?" ]]; then echo 1 ; else echo 0 ; fi
}
# elementSelectivity element position stat-file
function elementSelectivity {
    ./sgrep "$1 $2" $3 | cut -d' ' -f3
}
# tripleSelectivity subj pred obj stat-file nbtriple
function tripleSelectivity {
    if [[ $(isVar $1) == 0 ]];
    then
	tmp=$(elementSelectivity $1 "subj" $4)
	if [[ -z $tmp ]]; then selSubj=0 ; else selSubj=$tmp ; fi
    else
	selSubj=$5
    fi
    if [[ $(isVar $2) == 0 ]];
    then
	tmp=$(elementSelectivity $2 "pred" $4)
	if [[ -z $tmp ]]; then selPred=0 ; else selPred=$tmp ; fi
    else
	selPred=$5
    fi
    if [[ $(isVar $3) == 0 ]];
    then
	tmp=$(elementSelectivity $3 "obj" $4)
	if [[ -z $tmp ]]; then selObj=0 ; else selObj=$tmp ; fi
    else
	selObj=$5
    fi
    echo "$selSubj $selPred $selObj" | xargs -n1 | sort -n | head -n 1
}
##############################
##############################
if [[ $# != 2 ]];
then
    echo "Usage: $0 localQueryFile localStatisticalFile"
    exit 1
fi
##############################
##############################
BEGINQ=$(cat $1 | tr '\n' ' ' | tr '\t' ' ' | sed 's/[ ]* / /g' | sed 's/\(.*\)[Ww][Hh][Ee][Rr][Ee] {\(.*\)}.*/\1/g')
BGP=$(cat $1 | tr '\n' ' '  | tr '\t' ' ' | sed 's/[ ]* / /g' | sed 's/\(.*\)[Ww][Hh][Ee][Rr][Ee] {\(.*\)}\(.*\)/\2/g')
ENDQ=$(cat $1 | tr '\n' ' '  | tr '\t' ' '| sed 's/[ ]* / /g' | sed 's/\(.*\)[Ww][Hh][Ee][Rr][Ee] {\(.*\)}\(.*\)/\3/g')

NBTRIPLE=$(grep " pred " $2 | awk 'BEGIN{SUM=0}{SUM+=$3}END{print SUM}')

echo $BEGINQ " where {"
while read subj pred obj waste ; do
    echo "$(tripleSelectivity $subj $pred $obj $2 $NBTRIPLE)" $subj $pred $obj " ."
done <<< "$(echo "$BGP" | xargs -n4)" | sort -nk1,1 | cut -d' ' -f2-
echo " } " $ENDQ

exit 0
