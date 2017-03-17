import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import scala.util.matching.Regex

object GenerateStat {


  var numberMax = 100;

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


  def combine(acc:List[(Int,String)],size:Int,el:(Int,String)) : (List[(Int,String)],Int) = {
     val na = el::acc ;
     if(size+1==numberMax*2) {
         (na.sortWith(_._1._1>_._2._1).slice(0,numberMax),numberMax)
     }
     else {
       (na,size+1)
     }   
  }

  def main(args: Array[String]) {
    // Cut of spark logs.
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    val reg = new Regex("\\s+.\\s*$") ;
    val conf = new SparkConf().setAppName("Simple Application");
    val sc = new SparkContext(conf);

    val file = args(0)
    numberMax = args(1).toInt
    val t = sc.textFile(args(0)).flatMap{
        line =>
             val field:Array[String]=line.split("\\s+",3);
                 if(field.length!=3) {
                    throw new RuntimeException("Invalid line: "+line);
                 }
                 else {
                    List(((0,field(0)),1),((1,field(1)),1),((2,reg.replaceFirstIn(field(2),"")),1))
                 }
      }.reduceByKey(_+_).map { t => (t._1._1,(t._2,t._1._2))
      }.aggregateByKey( (Nil:List[(Int,String)],0) ) (
        { case ((acc,size),el) => combine(acc,size,el) },
        { case ((a1,s1),(a2,s2)) => (merge(a1,a2,numberMax),((s1+s2) min numberMax)) }
        ).collect
   for(i <- 0 to 2)
   {
     val (k,(v,s))=t(i);
     println(k.toString+" "+s) ;
     v foreach { case (n,iri) => println (iri+" "+n.toString) }  ;
   }
  }
}
