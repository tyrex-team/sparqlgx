#!/bin/bash

echo "Checking dependencies..."
for i in sbt ocamlopt hadoop spark-submit; do
    if [[ -z $(which $i) ]];
    then echo -e "[Fail]\t\t $i doesn't exist.";
    else echo -e "[Success]\t $i exists.";
    fi
done
echo ""
exit 0
