open Lexer ;;
open Parser ;;
open Algebra ;;

let file = ref "" 
let vertical = ref true   

let _ =
  
  if (Array.length Sys.argv) <= 1
  then
    failwith "Not enough args!" ;
  file := Sys.argv.(1) ;
  for i = 2 to Array.length Sys.argv -1 do
    if Sys.argv.(i) = "onefile" then vertical:=false ;
  done ;
  try
    let c = open_in (!file) in
    let lb = Lexing.from_channel c in
    let distinguished,term =  (Parser.query Lexer.next_token lb) in
    match distinguished with
    | ["*"] -> print_algebra (translate (!vertical) term)
    | _ -> print_algebra (Keep(distinguished,translate (!vertical) term))
  with
  | Lexer.Lexing_error s ->
     print_string ("lexical error: "^s);
     exit 1
  | Parser.Error ->
     print_string ("Parsing error (around line "^(string_of_int (!Lexer.line))^")\n");
     exit 1
          
  
