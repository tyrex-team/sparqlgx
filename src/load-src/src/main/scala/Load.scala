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
import java.io._

class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
    override def generateActualKey(key: Any, value: Any): Any = 
          NullWritable.get()

      override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = 
            key.asInstanceOf[String]+"/"+name
}


object Main {

  var stat_size = 500 ;

  def merge(xs: List[(Int,String)], ys: List[(Int,String)],n:Int): List[(Int,String)] = {
    if (n==0) { Nil }
    else
      (xs, ys) match {
        case(Nil, Nil) => Nil
        case(Nil, y::ys) => y::merge(Nil,ys,n-1)
        case(x::xs, Nil) => x::merge(xs,Nil,n-1)
        case(x :: xs1, y :: ys1) =>
          if (x._1 > y._1) x::merge(xs1, ys,n-1)
          else y :: merge(xs, ys1,n-1)
      }
  }



  def load(input:RDD[(String,String,String)], path:String) {
    val T = input.map {
      case (s,p,o) => 
        ("p"+p.toLowerCase.map{ 
          case c =>
            if( (c<'a' || c>'z') && (c<'0' || c>'9'))
              '_'
            else 
              c
        },(s+" "+o,""))};
    T.saveAsHadoopFile(path,classOf[String],classOf[String],classOf[RDDMultipleTextOutputFormat])
  }


  def stat(input:RDD[(String,String,String)], path:String) {
    val output = new BufferedWriter(new FileWriter(path))
    val stat = input
      .flatMap{ case (s,p,o) => List(((0,s),1),((1,p),1),((2,o),1)) }
      .reduceByKey(_+_).map { t => (t._1._1,(t._2,t._1._2))} // Compute word count
      .aggregateByKey( (Nil:List[(Int,String)],0) ) ( // Compute the stat_size most present per key (and key is s or p or o)
      { case ((acc,size),el) =>  (merge(el::Nil,acc,stat_size),(size+1) min stat_size) },
      { case ((a1,s1),(a2,s2)) => (merge(a1,a2,stat_size),((s1+s2) min stat_size)) }
    ).collect.sortWith{case (a,b) => a._1<b._1 }
    output.write(stat(0)._2._2.toString+" "+stat(1)._2._2.toString+" "+stat(2)._2._2.toString+"\n");
    for(i <- 0 to 2) {
      val list_el=stat(i)._2._1;
      val last = if(stat(i)._2._2 < stat_size) {"*"} else {list_el.last._2};
      list_el foreach { case (n,iri) => val v = if(iri==last) "*" else iri ;  output.write (v+" "+n.toString+"\n") } ;
    }
  }

  def fullstat(input:RDD[(String,String,String)], path:String) {
    val output = new BufferedWriter(new FileWriter(path))
    val stat = input.flatMap{ case (s,p,o) => List(((0,p,s),1),((1,p,o),1))}
      .reduceByKey(_+_).map { t => ((t._1._2,t._1._1),(t._2,t._1._3))} // Compute word count
      .aggregateByKey( (Nil:List[(Int,String)],0,0,0) ) ( //
      { case ((acc,size,nbDif,total),el) => (merge(el::Nil,acc,stat_size),(size+1) min stat_size,nbDif+1,total+el._1) },
      { case ((a1,s1,n1,t1),(a2,s2,n2,t2)) => (merge(a1,a2,stat_size),((s1+s2) min stat_size),n1+n2,t1+t2) }
    ).collect.foreach {
      case ((pred,col),(statlist,size,nbDif,total)) => 
        if(size<stat_size) {
          output.write(pred+" "+col+" "+(size+1).toString+" "+0+" "+total+"\n");
          statlist foreach { case (n,iri) => output.write(n.toString+" "+iri+"\n") } ;
          output.write("0 *\n");
        }
        else {
          output.write(pred+" "+col+" "+size.toString+" "+(nbDif-size+1).toString+" "+total+"\n");
          val last = statlist.last._2;
          statlist foreach { case (n,iri) => val v = if(iri==last) "*" else iri ;  output.write(n.toString+" "+v+"\n") } ;
        }
    }
  }

  def main(args: Array[String]) {
    // Cut of spark logs.
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    val reg = new Regex("\\s+.\\s*$") ;
    val conf = new SparkConf().setAppName("Simple Application");
    val sc = new SparkContext(conf);

    var load_path : Option[String] = None ;
    var stat_path : Option[String] = None ;
    var fullstat_path : Option[String] = None ;
    var prefix_path : Option[String] = None ;
    var tripleFile = "" ;
    var curArg = 0 ;
    while(curArg < args.length) {
      args(curArg) match {
        case "--load" =>
          if(curArg+1 == args.length)
            throw new Exception("No hdfs-path to load given!");
          load_path=Some(args(curArg+1))
          curArg+=1;
        case "--stat-size" =>
          if(curArg+1 == args.length)
            throw new Exception("No stat size given!");
          stat_size=args(curArg+1).toInt ;
          curArg+=1 ;
        case "--stat" =>
          if(curArg+1 == args.length)
            throw new Exception("No file to store stats given!");
          stat_path=Some(args(curArg+1)) ;
          curArg+=1 ;
        case "--full-stat" =>
          if(curArg+1 == args.length)
            throw new Exception("No file to store full stats given!");
          fullstat_path=Some(args(curArg+1)) ;
          curArg+=1 ;
        case "--prefix" =>
          if(curArg+1 == args.length)
            throw new Exception("No file to store prefixes given!");
          prefix_path=Some(args(curArg+1)) ;
          curArg+=1 ;
        case s =>
          if(tripleFile != "")
            throw new Exception("Invalid command line (two triple files given : "+s+" and "+tripleFile+")!");
          tripleFile = s
        }
        curArg+=1 ;
    }
    
    val input : RDD[(String,String,String)]= sc.textFile(tripleFile).map{ 
      line =>           
      val field:Array[String]=line.split("\\s+",3); 
      if(field.length!=3) {
        throw new RuntimeException("Invalid line: "+line);
      }
      (field(0),field(1),reg.replaceFirstIn(field(2),""))
    }.persist() ;


    load_path match {
      case None => ()
      case Some(path) => load(input,path)
    }

    stat_path match {
      case None => ()
      case Some(path) => stat(input,path)
    }

    fullstat_path match {
      case None => ()
      case Some(path) => fullstat(input,path)
    }}
}
