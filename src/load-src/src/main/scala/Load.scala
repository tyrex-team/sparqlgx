import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.log4j.Level
import org.apache.log4j.Level;
import org.apache.log4j.Logger
import org.apache.log4j.Logger;
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.Tuple2;
import scala.util.matching.Regex


class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
    override def generateActualKey(key: Any, value: Any): Any = 
          NullWritable.get()

      override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = 
            key.asInstanceOf[String]+"/"+name
}


object Main {

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

  def main(args: Array[String]) {
    // Cut of spark logs.
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    val reg = new Regex("\\s+.\\s*$") ;
    val conf = new SparkConf().setAppName("Simple Application");
    val sc = new SparkContext(conf);

    var statsize = 500 ;
    var hdfs_path : String = "" ;
    var noLoad = false ;
    var noStat = false ;
    var fullStat = false ;
    var tripleFile = "" ;
    var curArg = 0 ;
    while(curArg < args.length) {
      args(curArg) match {
        case "--no-load" =>
          noLoad = true
        case "--hdfs-path" =>
          if(curArg+1 == args.length)
            throw new Exception("No hdfs-path given!");
          hdfs_path=args(curArg+1)
          curArg+=1;
        case "--stat-size" =>
          if(curArg+1 == args.length)
            throw new Exception("No stat size given!");
          statsize=args(curArg+1).toInt ;
          curArg+=1 ;
        case "--no-stat" =>
          noStat=true;
        case "--full-stat" =>
          fullStat=true;
          noStat=true;
        case s =>
          if(tripleFile != "")
            throw new Exception("Invalid command line (two triple files given)!");
          tripleFile = s
        }
        curArg+=1 ;
    }
    
    val numberMax = statsize ;
    val input = sc.textFile(tripleFile) ;

    if(!noLoad) {
      val T = input.map {
        line => 
          val field:Array[String]=line.split("\\s+",3); 
        if(field.length!=3) { 
          throw new RuntimeException("Invalid line: "+line);
        }
        else { 
          ("p"+field(1).toLowerCase.map{ 
            case c =>
              if( (c<'a' || c>'z') && (c<'0' || c>'9'))
                 '_'
              else 
                c
          },(field(0)+" "+reg.replaceFirstIn(field(2),"")))}};

      T.saveAsHadoopFile(hdfs_path,classOf[String],classOf[String],classOf[RDDMultipleTextOutputFormat])
    }

    if(!noStat) {
      val stat = input.flatMap{
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
        { case ((acc,size),el) =>  (merge(el::Nil,acc,numberMax),(size+1) min numberMax) },
        { case ((a1,s1),(a2,s2)) => (merge(a1,a2,numberMax),((s1+s2) min numberMax)) }
      ).collect.sortWith{case (a,b) => a._1<b._1 }

      println(stat(0)._2._2.toString+" "+stat(1)._2._2.toString+" "+stat(2)._2._2.toString);
      for(i <- 0 to 2) {
        val list_el=stat(i)._2._1;
        val last = list_el.last._2;
        list_el foreach { case (n,iri) => val v = if(iri==last) "*" else iri ;  println (v+" "+n.toString) } ;
        //println();
      }
    }

    if(fullStat) {
      val stat = input.flatMap{
        line =>
        val field:Array[String]=line.split("\\s+",3);
        if(field.length!=3) {
          throw new RuntimeException("Invalid line: "+line);
        }
        else {
          List(((0,field(1),field(0)),1),((1,field(1),reg.replaceFirstIn(field(2),"")),1))
        }
      }.reduceByKey(_+_).map { t => ((t._1._2,t._1._1),(t._2,t._1._3)) // Compute word count
                             }.aggregateByKey( (Nil:List[(Int,String)],0,0,0) ) ( //
        { case ((acc,size,nbDif,total),el) => (merge(el::Nil,acc,numberMax),(size+1) min numberMax,nbDif+1,total+el._1) },
        { case ((a1,s1,n1,t1),(a2,s2,n2,t2)) => (merge(a1,a2,numberMax),((s1+s2) min numberMax),n1+n2,t1+t2) }
      ).collect.foreach {
        case ((pred,col),(statlist,size,nbDif,total)) => 
        println(pred+" "+col+" "+size.toString+" "+(nbDif-size-1).toString+" "+total);
        val last = statlist.last._2;
        statlist foreach { case (n,iri) => val v = if(iri==last) "*" else iri ;  println (v+" "+n.toString) } ;
        //println();
      }
    }
  }
}
