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

    val file = args(0)
    val numberMax = args(1).toInt
    val T = sc.textFile(file)
              .map{line=>val t=line.split(" ",3); ("subj "+t(0)+"@#@pred "+t(1)+"@#@obj "+t(2).substring(0,t(2).lastIndexOf(" ")))}
	      .flatMap(line=>line.split("@#@"))
	      .map(word => (word, 1))
	      .reduceByKey(_+_)
	      .cache

    val Ts = T.filter{case (w,nb)=>w.split(" ")(0).equals("subj")}
              .map{case (w,nb)=>(w.split(" ")(1),nb)}
	      .keyBy{case (a,b)=>b}
	      .sortByKey(false)
	      .values
	      .take(numberMax-1)

    val Tp = T.filter{case (w,nb)=>w.split(" ")(0).equals("pred")}
              .map{case (w,nb)=>(w.split(" ")(1),nb)}
	      .keyBy{case (a,b)=>b}
	      .sortByKey(false)
	      .values
	      .take(numberMax-1)

    val To = T.filter{case (w,nb)=>w.split(" ")(0).equals("obj")}
              .map{case (w,nb)=>(w.split(" ")(1),nb)}
	      .keyBy{case (a,b)=>b}
	      .sortByKey(false)
	      .values
              .take(numberMax-1)

    println((Ts.length+1) +" "+ (Tp.length+1) +" "+ (To.length+1))
    ((Ts:+("*",Ts(Ts.length-1)._2)) ++ (Tp:+("*",Tp(Tp.length-1)._2)) ++ (To:+("*",To(To.length-1)._2))).foreach{case (w,n)=>println(w+" "+n)}

    //sc.textFile(args(0))
    //  .map{line=>val t=line.split(" "); (t(0)+" subj@#@"+t(1)+" pred@#@"+t(2)+" obj")}
    //  .flatMap(line=>line.split("@#@"))
    //  .map(word => (word, 1))
    //  .reduceByKey(_ + _)
    //  .sortByKey()
    //  .map{case (a,b)=>a+" "+b}
    //  .saveAsTextFile("sparqlgxstatistics.txt");

  }
}
