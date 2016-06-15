open Struct;;    (* Types used. *)
open Parser;;    (* SPARQL parser. *)
open Cost;;      (* Optimisation on triples patterns. *)
open ToScalaOneFile;;   (* To return the Scala commands. *)

let read_lines name : string list =
  let ic = open_in name in
  let try_read () =
    try Some (input_line ic) with End_of_file -> None in
  let rec loop acc = match try_read () with
    | Some s -> loop (s :: acc)
    | None -> close_in ic; List.rev acc in
  loop []
;;

let translator ( (v:string) , (hdfsAddr:string) ) = 
  let q = parse v in
  let initEnv = {current=0;suite=[]} in
  (*let contrainte,newEnv = where ( no_order(q.const) , initEnv ) in*)
  let debut = "val triples=sc.textFile(\""^hdfsAddr^"\").map{line => val field:Array[String]=line.split(\" \"); (field(0),field(1),field(2))};\n" in
  let contrainte,newEnv = where ( order_by_nb_var(q.const) , initEnv ) in
  let selection = select ( q.dist , newEnv ) in
  let solution_modifiers = modifiers() in

  Printf.printf "%s \n" (debut^contrainte^selection^solution_modifiers)
;;

let () = 
  let l = read_lines Sys.argv.(2) in
  let rec aux l = match l with
    | [] -> Printf.printf ""
    | t :: q -> translator(t,Sys.argv.(1)); Printf.printf "\n"; aux q
  in aux l
;;

let ancien() =
  let ic = open_in Sys.argv.(2) in
  try 
    let line = input_line ic in  (* read line from in_channel and discard \n *)
    print_endline line;          (* write the result to stdout *)
    Printf.printf "\n";
    flush stdout;                (* write on the underlying device now *)
    close_in ic;                  (* close the input channel *) 
      
    translator(line,Sys.argv.(1))
      
  with e ->                      (* some unexpected exception occurs *)
    close_in_noerr ic;           (* emergency closing *)
    raise e                      (* exit with error: files are closed but
                                    channels are not flushed *)
;;
