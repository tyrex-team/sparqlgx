import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import scala.util.matching.Regex

object GenerateStat {

  def merge(xs: List[(Int,String)], ys: List[(Int,String)],n:Int): List[(Int,String)] = {
    if (n==0) { Nil }
    else
      (xs, ys) match {
        case(Nil, ys) => ys
        case(xs, Nil) => xs
        case(x :: xs1, y :: ys1) =>
          if (x._1 > y._1) x::merge(xs1, ys,n-1)
          else y :: merge(xs, ys1,n-1)
      }
  }


  def combine(acc:List[(Int,String)],size:Int,el:(Int,String),numberMax:Int) : (List[(Int,String)],Int) = {
     val na = merge(el::Nil,acc,numberMax) ;
       (na,(size+1) min numberMax)
  }

  def main(args: Array[String]) {
    // Cut of spark logs.
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    val reg = new Regex("\\s+.\\s*$") ;
    val conf = new SparkConf().setAppName("Simple Application");
    val sc = new SparkContext(conf);

    val file = args(0) ;
    val numberMax = args(1).toInt ;
    val t = sc.textFile(args(0)).flatMap{
        line =>
             val field:Array[String]=line.split("\\s+",3);
                 if(field.length!=3) {
                    throw new RuntimeException("Invalid line: "+line);
                 }
                 else {
                    List(((0,field(0)),1),((1,field(1)),1),((2,reg.replaceFirstIn(field(2),"")),1))
                 }
    }.reduceByKey(_+_).map { t => (t._1._1,(t._2,t._1._2)) // Compute word count
      }.aggregateByKey( (Nil:List[(Int,String)],0) ) ( // Compute the numberMax most present per key (and key is s or p or o)
        { case ((acc,size),el) => combine(acc,size,el,numberMax) },
        { case ((a1,s1),(a2,s2)) => (merge(a1,a2,numberMax),((s1+s2) min numberMax)) }
      ).collect.sortWith{case (a,b) => a._1<b._1 }

     println(t(0)._2._2.toString+" "+t(1)._2._2.toString+" "+t(2)._2._2.toString);
     for(i <- 0 to 2) {
       val list_el=t(i)._2._1;
       val last = list_el.last._2;
       list_el foreach { case (n,iri) => val v = if(iri==last) "*" else iri ;  println (v+" "+n.toString) } ;
       println();
       }
  }
}
