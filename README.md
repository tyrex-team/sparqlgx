SPARQLGX
========

`A distributed RDF store mapping SPARQL to Spark.`

__Overview__: SPARQL is the W3C standard query language for
querying data expressed in the Resource Description Framework
(RDF). The increasing amounts of RDF data available raise a major need
and research interest in building efficient and scalable distributed
SPARQL query evaluators. <br/> In this context, we propose and share
SPARQLGX: our implementation of a distributed RDF datastore based on
Apache Spark. SPARQLGX is designed to leverage existing Hadoop
infrastructures for evaluating SPARQL queries. SPARQLGX relies on a
translation of SPARQL queries into executable Spark code that adopts
evaluation strategies according to (1) the storage method used and (2)
statistics on data. Using a simple design, SPARQLGX already represents
an interesting alternative in several scenarios.  </p> </div>
      
__Version__: 1.0

Requirements
------------

- [Apache Hadoop](http://hadoop.apache.org) (+HDFS) version __2.6.0-cdh5.7.0__
- [Apache Spark](http://spark.apache.org/) version __1.6.0__
- [OCaml](http://ocaml.org/) version __4.02.2__

How to use it?
--------------

In this package, we provide sources to load and query RDF
datasets with SPARQLGX and S-DE (a SPARQL direct
evaluator). We also present a test-suite where two famous
RDF/SPARQL benchmarks can be runned : [LUBM](http://swat.cse.lehigh.edu/projects/lubm/) and [WatDiv](http://dsg.uwaterloo.ca/watdiv/). For space reasons, these two datasets only contain a few hundred of thousand RDF triples.

1. Get the sources and compile:
>git clone github.com/dgraux/sparqlgx.git  
>cd sparqlgx/ ;  
>bash dependencies.sh ;  
>bash compile.sh ;  

2. Load an RDF dataset: SPARQLGX can only load RDF data written according to
	  the [N-Triples
	  format](https://www.w3.org/TR/n-triples/); however, as many datasets come in other
	  standards (e.g. RDF/XML...) we also provide a .jar
	  file from an external developer able to translate RDF data
	  from a standard to an other one.
>hadoop fs -copyFromLocal local_file.nt hdfs_file.nt ;  
>hadoop fs -mkdir dataset_hdfs_dir/ ;  
>spark-submit --class "Load" bin/sparqlgx-load-0.1.jar hdfs_file.nt dataset_hdfs_dir/ ;  

3. Execute a SPARQL query: To execute a SPARQL
	  query over a loaded RDF dataset, users first need to
	  translate it into executable Spark-Scala-code. Then, the
	  Spark engine is able to evaluate the generated sequence. The
	  following command summarizes this entire process:
>bash bin/sparqlgx-eval.sh local_query.rq dataset_hdfs_dir/ ;  

4. Use the additional tools: Moreover, we give
	  two supplementary modules as extensions.
	  - A SPARQL query re-writer based on RDF data
	  statistics. Users first have to compute statistics; and then
	  SPARQL query clauses (Triple Patterns in the WHERE{...}) can
	  be re-ordered according to the data repartition.</li>
>bash bin/generate-stat.sh hdfs_file.nt output_local_stat.txt # Generate Statistics  
>bash bin/order-with-stat.sh local_query.rq local_stat.txt # Use Statistics  
	  - A SPARQL Direct Evaluator (S-DE) i.e. no loading
	  phase needed. S-DE directly evaluates SPARQL queries of RDF
	  datasets saved on the HDFS.
>bash bin/sparqlgx-direct-eval.sh local_query.rq hdfs_file.nt ;  

5. Run the test-suite:
>cd tests/ ;  
>bash run-me-first.sh ;          # Creates a workspace on the HDFS  
>bash run-sde-watdiv.sh ;        # Directly evaluates L1 on watdiv.nt  
>bash run-sparqlgx-lubm.sh ;     # Loads lubm.nt and executes Q1  
>bash run-sparqlgx-watdiv.sh ;   # Loads watdiv.nt and executes C1  
>bash run-statistics.sh ;        # Generates statistic files for the two datasets  
</pre>

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

[Tyrex](tyrex.inria.fr) Team, Inria (France), 2016