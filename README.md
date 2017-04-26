SPARQLGX
========

>Efficient Distributed Evaluation of SPARQL with Apache Spark.

__Overview:__ SPARQL is the W3C standard query language for querying
data expressed in
[RDF](https://www.w3.org/TR/2014/REC-rdf11-concepts-20140225/)
(Resource Description Framework). The increasing amounts of RDF data
available raise a major need and research interest in building
efficient and scalable distributed
[SPARQL](https://www.w3.org/TR/sparql11-query/) query evaluators.

In this context, we propose and share SPARQLGX: our implementation of
a distributed RDF datastore based on Apache Spark. SPARQLGX is
designed to leverage existing Hadoop infrastructures for evaluating
SPARQL queries. SPARQLGX relies on a translation of SPARQL queries
into executable Spark code that adopts evaluation strategies according
to (1) the storage method used and (2) statistics on data. Using a
simple design, SPARQLGX already represents an interesting alternative
in several scenarios.

__Version:__ 1.0 (A change log is available in ``CHANGES``.)

Related Publications
--------------------

- Damien Graux, Louis Jachiet, Pierre Genev&egrave;s, Nabil
  Laya&iuml;da. __SPARQLGX: Efficient Distributed Evaluation of SPARQL
  with Apache Spark__. _The 15th International Semantic Web
  Conference, Oct 2016, Kobe,
  Japan_. [link](https://hal.inria.fr/hal-01344915)

- Damien Graux, Louis Jachiet, Pierre Genev&egrave;s, Nabil
  Laya&iuml;da. __SPARQLGX in action: Efficient Distributed Evaluation
  of SPARQL with Apache Spark__. _The 15th International Semantic Web
  Conference, Oct 2016, Kobe,
  Japan_. [link](https://hal.inria.fr/hal-01358125)

- Damien Graux, Louis Jachiet, Pierre Genev&egrave;s, Nabil
  Laya&iuml;da. __SPARQLGX : Une Solution Distribuée pour RDF
  Traduisant SPARQL vers Spark__. _32ème Conférence sur la Gestion de
  Données - Principes, Technologies et Applications, Nov 2016,
  Poitiers, France_. [link](https://hal.inria.fr/hal-01412035)

Requirements
------------

- [Apache Hadoop](http://hadoop.apache.org) (+HDFS) version __2.6.0-cdh5.7.0__
- [Apache Spark](http://spark.apache.org/) version __1.6.0__
- [OCaml](http://ocaml.org/) version ≥ __4.0__
- [Menhir](http://gallium.inria.fr/~fpottier/menhir/) version __20160526__

How to use it?
--------------

In this package, we provide sources to load and query RDF datasets
with SPARQLGX and SDE (a SPARQL direct evaluator). We also present a
test-suite where two popular RDF/SPARQL benchmarks can be run:
[LUBM](http://swat.cse.lehigh.edu/projects/lubm/) and
[WatDiv](http://dsg.uwaterloo.ca/watdiv/). For space reasons, these
two datasets only contain a few hundred of thousand RDF triples, but
determinist generators are available on benchmarks' webpages.

### Use the provided Dockerfile

We provide a Dockerfile to compile and test SPARQLGX in a Docker
container.

It can be built then run with the following command lines:

    docker build -t sparqlgx .
    docker run -it sparqlgx

The image contains an installation of Hadoop and Spark, according
to the versions specified in ``conf/``.
They are respectively stored in ``/opt/hadoop`` and ``/opt/spark``.

SPARQLGX is installed in ``/opt/sparqlgx``, which is the home
directory of the user with same name.
All the required tools are installed, so that SPARQLGX can be
rebuilt using ``bash compile.sh`` as described in the next section.

By default, ``spark-submit`` will run the computations locally
(``local[*]``). This can be changed by mounting a configuration
file as ``/opt/spark/conf/spark-defaults.conf``.

The configuration of SPARQLGX can be changed by mounting a file
as ``/opt/sparqlgx/conf/sparqlgx.conf``.

### Get the sources, compile and configure.

Firstly, clone this repository. Secondly, check that all the needed
commands are available on your (main) machine and that your HDFS is
correctly configured. Thirdly, compile the whole project. Fourthly,
you can modify the parameters listed in the configuration file (in
`conf/`) according to your own cluster.

    git clone github.com/tyrex-team/sparqlgx.git
    cd sparqlgx/ ;
    bash dependencies.sh ;
    bash compile.sh ;

### Load an RDF dataset.

SPARQLGX can only load RDF data written according to the [N-Triples
format](https://www.w3.org/TR/n-triples/); however, as many datasets
come in other standards (e.g. RDF/XML...) we also provide a .jar file
(rdf2rdf in `bin/`) from an external developer able to translate RDF
data from a standard to an other one.

Before, loading an RDF triple file, you have to copy it directly on
the HDFS. Then, the complete preprocessing routine can be realized
using the `load` parameter; it will partition the HDFS triple file and
compute statistics on data. These two distinct steps can be executed
separately with respectively `light-load` and `generate-stat`.

    hadoop fs -copyFromLocal local_file.nt hdfs_file.nt ;
    bash bin/sparqlgx.sh light-load dbName hdfs_file.nt ;
    bash bin/sparqlgx.sh generate-stat dbName hdfs_file.nt ;
    bash bin/sparqlgx.sh load dbName hdfs_file.nt ;

### Remove a dataset.

    bash bin/sparqlgx.sh remove dbName ;

### Execute a SPARQL query.

To execute a SPARQL query over a loaded RDF dataset, users can use
`query` which translates the SPARQL query into Scala, compiles it and
runs it with Apache Spark. Moreover, users can call SDE (the SPARQLGX
Direct Evaluator) with `direct-query` which directly evaluates SPARQL
queries on RDF datasets saved on the HDFS. Finally, three levels of
optimizations are available:

1. __No Optimization__ If `--no-optim` is specified in the command
line, SPARQLGX or SDE will execute the given query following exactly
the order of clauses in the WHERE.

2. __Avoid Cartesian Products__ [Default] If no optimization option is
given to either SPARQLGX or SDE, the translation engine will try to
avoid (if possible) cartesian product in its translation output.

3. __Query Planning with Statistics__ The `--stat` option is only
available with SPARQLGX since SDE directly evaluates queries without
preprocessing phase; in addition, you should have already generate
statistics either with `generate-stat` or with `load`. It will imply a
reordering of SPARQL query clauses (triple patterns in the WHERE{...})
according to data repartition. Finally, it will also try to avoid
cartesian products from the new statistic-based order.

<!-- Dirty Markdown Hack -->

    bash bin/sparqlgx.sh query dbName local_query.rq ;
    bash bin/sparqlgx.sh direct-query local_query.rq hdfs_file.nt ;

### Translate Only (for debugging).

It is also possible to translate only the SPARQL query (without
executing the output) into the Scala code. This routine returns the
origin query, the one that is actually translated after the potential
optimizations (`--no-optim` or `--stat` or nothing) and the obtained
Scala code.

    bash bin/sparqlgx.sh translate dbName local_query.rq ;

### Run the test suite.

We also provide a basic test suite using two popular benchmarks (LUBM
and WatDiv). To that purpose, we pre-generated two small RDF datasets
and give the various queries required for these benchmarks. The test
suite is divided into three parts: `preliminaries.sh` sets up files
and directories on the HDFS, `run-benchmarks.sh` executes everything
(preprocessing, querying with SDE or SPARQLGX and with various
optimization options), `clean-all.sh` puts everything back in place.

    cd tests/ ;
    bash preliminaries.sh ;
    bash run-benchmarks.sh ; # This step can take a while!
    bash clean-all.sh

License
-------

This project is under the [CeCILL](http://www.cecill.info/index.en.html) license.

Authors
-------

Damien Graux  
<damien.graux@inria.fr>  

Louis Jachiet  
Pierre Genev&egrave;s  
Nabil Laya&iuml;da  

[Tyrex Team](http://tyrex.inria.fr), Inria (France), 2017
