import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Load {
  def main(args: Array[String]) {
    // Cut of spark logs.
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    val conf = new SparkConf().setAppName("Simple Application");
    val sc = new SparkContext(conf);
    val T = sc.textFile(args(0)).map{line => val field:Array[String]=line.split(" ",3); (field(0),field(1),field(2).substring(0,field(2).lastIndexOf(" ")))}.cache;
    //val T = sc.textFile(args(0)).map{line => val field:Array[String]=line.split(" "); (field(0),field(1),field(2))}.cache;

    val confhadoop = sc.hadoopConfiguration
    val fshadoop = org.apache.hadoop.fs.FileSystem.get(confhadoop)

    val Apred = T.map{case (s,p,o)=>p}.distinct.collect;
    val Avalue = Apred.map{case lambda => lambda.toLowerCase.map{ case c =>
          if( (c<'a' || c>'z') && (c<'0' || c>'9')){ 'z'} else {c}
          }};
    
    for( i <- 0 to Apred.length -1){
      val exists = fshadoop.exists(new org.apache.hadoop.fs.Path(args(1)+Avalue(i)+".pred"))
      if(!exists){
        T.filter{case (s,p,o) => p.equals(Apred(i))}.map{case (s,p,o) => s + " " + o}.saveAsTextFile(args(1)+Avalue(i)+".pred");
      }
    }
  }
}
