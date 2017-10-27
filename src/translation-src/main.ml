open Lexer ;;
open Parser ;;
open Algebra ;;
open Reorder ;;
open Translate ;;
(*
 Brief:
   sparql2scala translates SPARQL query files into Scala code able to
   be run by Apache Spark.
 Usage:
   > ./sparql2scala queryFile [onefile] [ [-no-optim] | [-stat statFile] ] [-debug]
 Options:
      onfile       to translate files without using vertical partitioning
      -no-optim    to exactly respect the join order induced by the file
      -stat file   to benefit from statistics in the join order
      -debug       to print intermediate results and additional data
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
let debug = ref false
let plan = ref false 

let rec parse_arg = function
  | [] -> ()
  | "--onefile"::q -> vertical:=false ; parse_arg q
  | "--debug"::q -> debug:=true ; parse_arg q
  | "--no-optim"::q -> optim := 0 ; parse_arg q
  | "--stat"::s::q -> Reorder.load s ; optim:=2 ; parse_arg q
  | "--restricted-stat"::k::s::q-> Reorder.load_rest_stat k s ; optim:=3 ; parse_arg q
  | "--fullstat"::s::q
  | "--full-stat"::s::q-> Reorder.load_full_stat s ; optim:=3 ; parse_arg q
  | "--prefix"::s::q -> Prefix.load s ; parse_arg q
  | "--plan"::q
    | "--json"::q-> plan:=true ; parse_arg q
  | f::q ->
     if !file = ""
     then file := f
     else failwith ("Unrecognized arg "^f^" (guessed file was "^(!file)^"?)") ;
     parse_arg q
          
let _ =
  
  parse_arg (List.tl (Array.to_list Sys.argv)) ;
  try
    let c = open_in (!file) in
    let lb = Lexing.from_channel c in
    let (distinguished,term),modifiers =  (Parser.query Lexer.next_token lb) in
    let translated = (translate distinguished modifiers (!vertical) (!optim) term) in
    if (!debug) then
     begin
      print_string "\nInitial Query is:\n-----------------\n" ;
    let translated_stupid = (translate distinguished modifiers (!vertical) 0 term) in
      print_query distinguished modifiers 0 translated_stupid ;
      print_string "\nOptimized Query is:\n-------------------\n" ;
      print_query distinguished modifiers (!optim) translated ;
      print_string "\nObtained Scala Code is:\n-----------------------\n" ;
     end ;
    if !plan
    then
      JsonOutput.print translated
    else
      ScalaOutput.print translated
  with
  | Lexer.Lexing_error s ->
     print_string ("lexical error: "^s);
     exit 1
  | Parser.Error ->
     print_string ("Parsing error (around line "^(string_of_int (!Lexer.line))^")\n");
     exit 1
  | Sparql.TypeError(c) ->
     print_string ("Typing error (collumn '"^c^"' is not certain around line "^(string_of_int (!Lexer.line))^")\n");
     exit 1
          
  
