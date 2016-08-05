open Lexer ;;
open Parser ;;
open Algebra ;;
open Reorder ;;

(*
 Brief:
   sparql2scala translates SPARQL query files into Scala code able to
   be run by Apache Spark.
 Usage:
   > ./sparql2scala queryFile [onefile] [ [-no-optim] | [-stat statFile] ]
 Options:
      onfile       to translate files without using vertical partitioning
      -no-optim    to exactly respect the join order induced by the file
      -stat file   to benefit from statistics in the join order
 Note:
   There are three levels of optimizations: the first one respect the
   order of conditions in the query file and optimizes nothing, the
   second one (default) tries to avoid if possible cartesian products,
   the third one which needs an additional statistic file aims at
   ordering conditions by selectivity.
*)

let file = ref ""
let vertical = ref true
let optim = ref 1

let _ =
  
  if (Array.length Sys.argv) <= 1
  then
    failwith "Not enough args!" ;
  file := Sys.argv.(1) ;
  for i = 2 to Array.length Sys.argv -1 do
    if Sys.argv.(i) = "onefile" then vertical:=false ;
    if Sys.argv.(i) = "-no-optim" then optim:=0 ;
    if Sys.argv.(i) = "-stat"
    then if Array.length Sys.argv > i+1
         then begin Reorder.load Sys.argv.(i+1) ; optim:=2 ; end ;
  done ;
  try
    let c = open_in (!file) in
    let lb = Lexing.from_channel c in
    let (distinguished,term),modifiers =  (Parser.query Lexer.next_token lb) in
    print_algebra (translate distinguished modifiers (!vertical) (!optim) term)
  with
  | Lexer.Lexing_error s ->
     print_string ("lexical error: "^s);
     exit 1
  | Parser.Error ->
     print_string ("Parsing error (around line "^(string_of_int (!Lexer.line))^")\n");
     exit 1
          
  
