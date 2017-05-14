open Sparql ;;
open Yojson ;;

type algebra =
  | Readfile3 
  | Readfile2 of string
  | Filter of string * string * algebra
  | Keep of string list * algebra
  | Join of algebra * algebra
  | Union of algebra * algebra
  | LeftJoin of algebra * algebra
  | JoinWithBroadcast of algebra * algebra
  | Broadcast of int*algebra*algebra
  | FilterWithBroadcast of algebra * int * string list
  | Rename of string * string * algebra
  | Distinct of algebra
  | Order of (string*bool) list*algebra
  | Empty
  
(* Numero hashes predicate names; in Vertical partionning the
   (subject,object) associated with pred are stored in (numero
  pred)^".pred "*)
  let numero(s:string):string=
     String.map (fun c -> if (c <'a' || c>'z') && (c<'0' || c>'9') then '_' else c) (String.lowercase s)


  
let rec print_algebra term = 
  
  let gid = 
    let id = ref 0 in
    fun () -> incr id ; string_of_int (!id) 
  in
  
  let lines = ref [] in
  
  let add l = lines := ("    "^l^"\n")::(!lines) in 

  let header = "
import scala.util.matching.Regex
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Query {
  def main(args: Array[String]) {
     Logger.getLogger(\"org\").setLevel(Level.OFF);
     Logger.getLogger(\"akka\").setLevel(Level.OFF);
     val conf = new SparkConf().setAppName(\"SPARQLGX Evaluation   \");
     val sc = new SparkContext(conf);
     if(args.length == 0) {
       throw new Exception(\"We need the path of the queried data!\")
     }
     def readpred (s:String) = 
                if(org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration).exists(new org.apache.hadoop.fs.Path(args(0)+\"/\"+s))) {
           sc.textFile(args(0)+\"/\"+s).map{line => val field:Array[String]=line.split(\" \",2); (field(0),field(1))}
        }
        else {
           sc.emptyRDD[(String,String)]
        };

        def readwhole () = { 
          val reg = new Regex(\"\\\\s+.\\\\s*$\") ;
          sc.textFile(args(0)).map{
               line => 
                   val field:Array[String]=line.split(\"\\\\s+\",3); 
                   if(field.length!=3) { 
                     throw new RuntimeException(\"Invalid line: \"+line);
                   }
                   else { 
                     (field(0),field(1),reg.replaceFirstIn(field(2),\"\"))
                   }
          }
      }
      val start_date = java.lang.System.currentTimeMillis();
"  in

  let footer = "
    if(args.length == 1 || args(1) == \"\" || args(1) == \"-\") {
       Qfinal.collect().foreach(println)
    }
    else {
       Qfinal.saveAsTextFile(args(1))
    }
   var time_taken = (java.lang.System.currentTimeMillis() - start_date).toDouble /1000 ; 
   println(\"Total time: \"+time_taken) ;
  }
}
" in
  
  let escape_var a =
    if a.[0] = '?'
    then let b = Bytes.copy a in (Bytes.set b 0 'v' ; b)
    else
      if a.[0] = '$'
      then let b = Bytes.copy a in (Bytes.set b 0 'd' ; b)
      else a
  in

  let rec join keys cols =
    let rec foo = function
      | [] -> ""
      | [a] -> escape_var a
      | a::q -> escape_var a^","^foo q
    in
    match (keys) with
    | [] -> foo cols
    | k  -> "(keys,("^foo cols^"))"
  in
  
  let typeof = function
    | [] -> failwith __LOC__
    | [a] -> "String"
    | l ->
       let rec foo = function
         | [] -> failwith __LOC__
         | [a] -> "String)"
         | a::q -> "String,"^foo q
       in
       "("^foo l
  in
  
  let mapkeys cols keys newkeys =
    let values = if keys = [] then "" else ".values" in
    if newkeys = []
    then values
    else
      if List.map (List.nth cols) keys = newkeys
      then ""
      else values^".keyBy{case ("^(join [] cols)^")=>("^(join [] newkeys)^")}"
  in
  
  let renamedup l1 l2 =
    (* bis_varname is not a possible variable name *)
    List.map (fun t -> if List.mem t l2 then "bis_"^(escape_var t) else t) l1
  in

  
  (*foo term returns (id,cols) where "V"id is the variable associated
  with term and cols is the list of columns of term (in the order they
  appear in the bdd *)

  let trad_one = Hashtbl.create 17 in

  let normalize_var_name code =
    let rename =
      let a = ref [] in
      let cur = ref 0 in
      fun s ->
      try
        List.assoc s (!a)
      with
        Not_found ->
        let trad=string_of_int (!cur) in
        a:= (s,trad)::!a ;
        incr cur ;
        trad
    in
    let rec foo = function
      | Filter(s,v,c) -> Filter(rename s, v,foo c)
      | Keep(l,c) -> Keep (List.map rename l,foo c)
      | Join(a,b) -> Join(foo a, foo b)
      | JoinWithBroadcast(a,b) -> JoinWithBroadcast(foo a, foo b)
      | Union(a,b) -> Union(foo a, foo b)
      | LeftJoin(a,b) -> LeftJoin(foo a, foo b)
      | Rename(o,n,c) -> Rename(rename o, rename n,foo c)
      | Distinct c -> Distinct (foo c)
      | Order(l,c) -> Order(List.map (fun (x,b) -> rename x,b) l,foo c)
      | v -> v
    in
    foo code
  in

  let rec cols = function
    | Readfile3 -> ["s";"p";"o"]
    | Readfile2(f) -> ["s";"o"]
                    
    | Distinct c
    | Order(_,c)
      | Filter(_,_,c) -> cols c
                       
    | Keep(k,Empty) -> []
    | Keep(k,c) -> k
    | FilterWithBroadcast(a,_,_) -> cols a
    | Broadcast(_,a,b) -> cols b
    | Union(a,b) 
      | LeftJoin(a,b)
      | Join(a,b)
      | JoinWithBroadcast(a,b) ->
       let c_a = cols a in
       ListSet.union c_a (cols b) 
       
    | Rename(o,n,Empty) -> []
    | Rename(o,n,c) -> List.map (fun x -> if x=o then n else x) (cols c)
    | Empty -> []
  in
       
  let rec foo l =
    let normalized = normalize_var_name l in
    try
      let id_code,by_keys=Hashtbl.find trad_one normalized in
      id_code,by_keys,cols l
    with
      Not_found ->        
        let res = "v"^gid () in 
        let code,keys,cols = match l with 
          | Readfile3 ->
             "val "^res^"=readwhole();",[],["s";"p";"o"]
          | Readfile2(f) ->
             "val "^res^"=readpred(\"p"^(numero f)^"\") //"^f,[],["s";"o"]
               
          | Filter(c,v,a) ->
             let code,keys,cols = foo a in
             "val "^res^"="^code^".filter{case ("^(join keys cols)^") => "^(escape_var c)^".equals("^(escape_var v)^")}",keys,cols
                                                                                                                  
          | Keep (keepcols,a) ->
             let code,keys,cols = foo a in
             if cols <> keepcols && cols != []
             then
               "val "^res^"="^code^".map{case ("^(join keys cols)^") => ("^(join [] keepcols)^")}",[],keepcols
             else
               "val "^res^"="^code^" // useless keepcols",keys,keepcols
               
          | LeftJoin(a,b) ->
             let code_a,keys_a,cols_a = foo a
             and code_b,keys_b,cols_b = foo b in
             let cols_join =  ListSet.inter cols_b cols_a in
             let cols_of_b = ListSet.minus cols_b cols_join in
             let cols_union_some = cols_a@(cols_of_b) in
             let cols_union_none = cols_a@(List.map (fun x -> "\"\"") cols_of_b) in
             let cols_b_bis = renamedup cols_b cols_a in
             if cols_join = []
             then
               "val "^res^"="^code_a^mapkeys cols_a keys_a []^".cartesian("^code_b^mapkeys cols_b keys_b []^").map{case (("^join [] cols_a^"),("^join [] cols_b^")) => ("^join [] cols_union_some^")}",[],cols_union_some
             else
               let cols_int = List.mapi (fun i s -> s,i) cols_union_some in 
               "val "^res^"="^code_a^mapkeys cols_a keys_a cols_join
               ^".leftOuterJoin("^code_b^mapkeys cols_b keys_b cols_join^")"
               ^".mapValues{case( ("^(join [] cols_a)^"), opt_b)=> opt_b match { case None => ("^(join [] cols_union_none)^") case Some( ("^join [] cols_b_bis^") ) => ("^join [] cols_union_some ^") }}",(List.map (fun s -> List.assoc s cols_int) cols_join),cols_union_some
             
          | Join(b,a) ->
             let code_a,keys_a,cols_a = foo a
             and code_b,keys_b,cols_b = foo b in
             let cols_join =  ListSet.inter cols_a cols_b in
             let cols_union = ListSet.union cols_a cols_b in
             let cols_b_bis = renamedup cols_b cols_a in
             if cols_join = []
              then
               "val "^res^"="^code_a^mapkeys cols_a keys_a []^".cartesian("^code_b^mapkeys cols_b keys_b []^").map{case (("^join [] cols_a^"),("^join [] cols_b^")) => ("^join [] cols_union^")}",[],cols_union
              else
                let cols_int = List.mapi (fun i s -> s,i) cols_union in 
                "val "^res^"="^code_a^mapkeys cols_a keys_a cols_join 
                ^".join("^code_b^mapkeys cols_b keys_b cols_join^")"
                ^".mapValues{case( ("^(join [] cols_a)^"),("^(join [] cols_b_bis)^"))=>("^(join [] cols_union)^")}",(List.map (fun s -> List.assoc s cols_int) cols_join),cols_union

          | JoinWithBroadcast(b,a) ->
             let code_a,keys_a,cols_a = foo a
             and nokey_a = code_a^mapkeys cols_a keys_a [] in
             and code_b,keys_b,cols_b = foo b in
             let cols_join = ListSet.inter cols_a cols_b in
             let cols_union = ListSet.union cols_b cols_a in
             let cols_a_spec  = ListSet.minus cols_a cols_join in
             if cols_join = []
              then
               "val "^res^"="^nokey_a^".cartesian("^code_b^mapkeys cols_b keys_b []^").map{case (("^join [] cols_a^"),("^join [] cols_b^")) => ("^join [] cols_union^")}",[],cols_union
             else
               if cols_a_spec = []
               then
                 let broadcast_a = "val broadcast_"^code_a^"=sc.broadcast("^nokey_a^".collect().toSet)\n" in
                 broadcast_a^
                   "val "^res^"="^code_b^".filter{ case ("^join keys_b cols_b^") => broadcast_"^code_a^".value("^join [] cols_join^")}", keys_b,cols_union
                 
               else
                 let set_a = "collection.mutable.Set["^typeof cols_a_spec^"]" in
                 let mmap_a = "val mmap_"^code_a^" = new collection.mutable.HashMap["^typeof cols_join^", "^set_a^"]() with collection.mutable.MultiMap[String, ("^typeof cols_a_spec^")]\n" in
                 let add_mmap_a = nokey_a^".collect().foreach { case ("^join [] cols_a^") => mmap_"^code_a^".addBinding( ("^join [] cols_join^"),("^join [] cols_a_spec^"))}\n" in
                 let isValues = if keys_b <> [] then "Values" else "" in
                 let broadcast_a = "val broadcast_"^code_a^"=sc.broadcast(mmap_"^code_a^")\n" in
                 mmap_a^add_mmap_a^broadcast_a^
                   "val "^res^"="^code_b^".flatMap"^isValues^"{ case ("^join [] cols_b^") => broadcast_"^code_a^".value.getOrElse( ("^join [] cols_join^"),"^set_a^"()).map{ case ("^join [] cols_a_spec^") => ("^join [] cols_union^") }}", keys_b,cols_union
          | FilterWithBroadcast(a,i,cols) ->
             let code_a,keys_a,cols_a = foo a in
             let cols_join = ListSet.inter cols_a cols in
             (* print_int i ; print_newline (); *)
             (* print_string "cols_a : "; List.iter (fun x -> print_string (x^" ")) cols_a ; print_string "\n"; *)
             (* print_string "cols : "; List.iter (fun x -> print_string (x^" "))  cols ; print_newline(); *)
             assert (cols_join <> []) ;
             assert (cols_join = cols);
             let broadcast_i = "broadcast_"^string_of_int i in
              "val "^res^"="^code_a^".filter{ case ("^join keys_a cols_a^") =>"^broadcast_i^".value("^join [] cols_join^")}", keys_a,cols_a
                 
          | Broadcast(i,a,b) ->
             let code_a,keys_a,cols_a = foo a in
             let isValues = match keys_a with | [] -> "" | _ -> ".values" in
             let broadcast_a = "val broadcast_"^(string_of_int i)^"=sc.broadcast("^code_a^isValues^".collect().toSet)\n" in
             add broadcast_a ;
             let code_b,keys_b,cols_b = foo b in
             "val "^res^"="^code_b,keys_b,cols_b 
          | Rename(o,n,c) ->
             let code_c,keys_c,cols_c = foo c in
             "val "^res^"="^code_c,keys_c,(List.map (fun x -> if x=o then n else x) cols_c)

          | Union (a,b) ->
             let code_a,keys_a,cols_a = foo a
             and code_b,keys_b,cols_b = foo b in
             let cols_union = ListSet.union cols_a cols_b in
             let new_cols_a = List.map (fun x -> if List.mem x cols_a then x else "\"\"") cols_union in
             let new_cols_b = List.map (fun x -> if List.mem x cols_b then x else "\"\"") cols_union in
             "val "^res^"= ("^code_a^mapkeys cols_a keys_a []^".map{case ("^(join [] cols_a)^")=>("^(join [] new_cols_a)^")}).union("^code_b^mapkeys cols_a keys_a []^".map{case("^(join [] cols_b)^") => ("^(join [] new_cols_b)^")})",[],cols_union
             
          | Distinct(a) ->
             let code_a,keys_a,cols_a = foo a in
             "val "^res^" ="^code_a^mapkeys cols_a keys_a []^".distinct() ",[],cols_a
          | Empty ->
             "val "^res^" = sc.emptyRDD[Int]",[],[]
          | Order(l,a) ->
             let code_a,keys_a,cols_a = foo a in
             let cols_sort = List.filter (fun x -> List.mem_assoc x l) cols_a in
             let type_sort = "("^join [] (List.map (fun s -> "String") cols_sort)^")" in
             let rec foo x = function
               | [] -> failwith "sort column not present!"
               | a::q -> if x=a then 1 else (1+foo x q)
             in
             let ith = List.map (fun (v,s) -> string_of_int (foo v cols_a),s) l in
             match cols_sort
             with
             | [] -> "val "^res^"="^code_a^mapkeys cols_a keys_a [],[],cols_a
             | [col_sort] ->
                let side = List.assoc col_sort l in 
                "val "^res^"="^code_a^mapkeys cols_a keys_a cols_sort^".sortByKey("^string_of_bool side^").values",[],cols_a
             | cols_sort ->
                begin
                  add ("implicit val specifiedOrdering = new Ordering["^type_sort^"] {") ;
                  add ("       override def compare(a: "^type_sort^", b: "^type_sort^") = " );
                  List.iter (fun (v,s) -> let side = if s then "" else "(-1)*" in
                                          add ("if ( a._"^v^" != b._"^v^" ) { "^side^"(a._"^v^".compare(b._"^v^")) } else ")) ith ;
                  add " { 0 } }" ;
                  "val "^res^" ="^code_a^mapkeys cols_a keys_a cols_sort^".sortByKey(true).values ",[],cols_a
                end
             
        in
        add code ;
        Hashtbl.add trad_one normalized (res,keys) ; res,keys,cols
  in

  add header ; 
  
  let code,cols = match foo term with
    | code,[],cols -> code,cols
    | code,_,cols -> code^".values",cols
  in
  add ("val Qfinal="^code) ;
  add footer ;     
  List.iter print_string (List.rev (!lines)) 
;;
  
let print_json a =
  let list_instr = ref [] in
  let add x = list_instr := x::!list_instr in
  let cur_id = ref (-1) in
  let id () = incr cur_id ; !cur_id in
  let rec foo cols x =
    let id = id() in
    let jid =  "id",`Int id in
    let op c = "op",`String c in
    let assoc v l =
      try
        List.assoc v l
      with
        Not_found -> v
    in

    let op c args = add (`Assoc  ["op",`String c; jid; "arg",`Assoc args]) ; id in
    
    match x with
     | Readfile3 ->
        op "ALL" ["cols",`List (List.map (fun x-> `String (assoc x cols)) ["s";"p";"o"]) ]
     | Readfile2(f) ->
        op "PRED"  ["filename",`String ("p"^numero f);"col_subject",`String (assoc  "s" cols); "col_object",`String (assoc  "o" cols) ]
     | Filter(c,v,a) ->
        op "FILTER"  ["col",`String (assoc c cols); "value",`String (String.sub v 1 (String.length v-2)) ; "id",`Int (foo cols a) ]
     | Keep(l,a) ->
        op "SELECT"  ["cols",`List (List.map (fun c -> `String (assoc c cols)) l) ; "id",`Int (foo cols a) ]
     | Join(a,b) ->
        op "JOIN" ["id1",`Int (foo cols a) ;  "id2",`Int (foo cols b)]
     | Rename(a,b,c) ->
        foo ((a,b)::cols) c
       
        
     | _ -> failwith ("error @"^__LOC__)
  in
  let final = foo [] a in
  print_string (Yojson.Basic.to_string (`Assoc ["commands",`List (List.rev !list_instr); "result_id",`Int final]))
