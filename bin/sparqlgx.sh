#!/bin/bash

PATH_CMD=$(dirname $0) ;

source ${PATH_CMD}/../conf/sparqlgx.conf

function version {
    echo "1.0"
}

function help_msg {
    echo "SPARQLGX version $(version)"
    echo ""
    echo "Usage:"
    echo "   $0 light-load dbName tripleFile_HDFSPath"
    echo "   $0 load dbName tripleFile_HDFSPath"
    echo "   $0 generate-stat dbName tripleFile_HDFSPath"
    echo "   $0 query [-o responseFile_HDFSPath] [--no-optim] [--stat] [--clean] dbName queryFile_LocalPath"
    echo "   $0 direct-query [-o responseFile_HDFSPath] [--no-optim] [--clean] queryFile_LocalPath tripleFile_HDFSPath"
    echo "   $0 remove dbName"
    echo ""
    echo "   $0 --version"
    echo "   $0 --help"
    echo ""
    echo "Tyrex <tyrex.inria.fr>"
    echo "2016"
}

case "$1" in
    version | -v | --version )
	echo "SPARQLGX version $(version)"
	;;
    help | -h | --help )
	help_msg
	;;
    light-load )
	shift
	bash ${PATH_CMD}/sgx-load.sh $@
	;;
    load )
	shift
	bash ${PATH_CMD}/sgx-load.sh $@
	bash ${PATH_CMD}/sgx-stat.sh $@
	;;
    remove )
	shift
	bash ${PATH_CMD}/sgx-purge.sh $@
	;;
    generate-stat )
	shift
	bash ${PATH_CMD}/sgx-stat.sh $@
	;;
    query )
	shift
	bash ${PATH_CMD}/sgx-eval.sh $@
	;;
    direct-query )
	shift
	bash ${PATH_CMD}/sde-eval.sh $@
	;;
    *)
	help_msg
	;;
esac

exit 0
