#!/bin/bash
cd $(dirname $0)
echo "Checking dependencies..."
for i in sbt menhir ocamlc ocamldep ocamllex ocamlopt hadoop spark-submit ocamlfind; do
    if [[ -z $(which $i) ]];	
    then echo -e "[Fail]\t\t$i doesn't exist.";
    else echo -e "[Success]\t$i exists.";
    fi
done 2> /dev/null

if (! [[ -z $(which ocamlfind) ]]) && [[ $(ocamlfind query yojson | grep "not found") -eq 0 ]] ;
then echo -e "[Success]\tyojson exists.";
else echo -e "[Fail]\t\tyojson doesn't exist.";
fi;
echo ""
exit 0
