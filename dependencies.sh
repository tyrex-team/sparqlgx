#!/bin/bash
cd $(dirname $0)
echo "Checking dependencies..."
for i in sbt menhir ocamlc ocamldep ocamllex ocamlopt hadoop spark-submit; do
    if [[ -z $(which $i) ]];	
    then echo -e "[Fail]\t\t$i doesn't exist.";
    else echo -e "[Success]\t$i exists.";
    fi
done 2> /dev/null
echo ""
exit 0
