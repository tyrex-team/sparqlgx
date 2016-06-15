import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

object GenerateStat {
  def main(args: Array[String]) {
    // Cut of spark logs.
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    val conf = new SparkConf().setAppName("Simple Application");
    val sc = new SparkContext(conf);

    sc.textFile(args(0))
      .map{line=>val t=line.split(" "); (t(0)+" subj@#@"+t(1)+" pred@#@"+t(2)+" obj")}
      .flatMap(line=>line.split("@#@"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortByKey()
      .map{case (a,b)=>a+" "+b}
      .saveAsTextFile("sparqlgxstatistics.txt");

  }
}
