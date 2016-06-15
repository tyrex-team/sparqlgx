open Struct;;    (* Types used. *)
open Parser;;    (* SPARQL parser. *)
open Cost;;      (* Optimisation on triples patterns. *)
open ToScalaVertical;;   (* To return the Scala commands. *)

let read_lines name : string list =
  let ic = open_in name in
  let try_read () =
    try Some (input_line ic) with End_of_file -> None in
  let rec loop acc = match try_read () with
    | Some s -> loop (s :: acc)
    | None -> close_in ic; List.rev acc in
  loop []
;;

let translator (v:string) = 
  let q = parse v in
  let initEnv = {current=0;suite=[]} in
  let definition = "def t (s:String):Boolean = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration).exists(new org.apache.hadoop.fs.Path(s))\n" in

  match q.union with
  | [] ->
     let preliminaires = definition^listLoaders(q.const) in
     let contrainte,newEnv = where ( no_order(q.const) , initEnv ) in
     (*let contrainte,newEnv = where ( order_by_nb_var(q.const) , initEnv ) in*)
     let selection = select ( q.dist , newEnv ) in
     let solution_modifiers = modifiers() in
     let finalquery = "val Qfinal = if (!("^listPaths(q.const)^")) {Array();} \n\t else{"^selection^solution_modifiers^"}" in
     Printf.printf "%s \n" (preliminaires^contrainte^finalquery)
  | union -> 
     let preliminaires = definition^listLoaders(q.const) in
     let contrainte,newEnv = where ( no_order(q.const) , initEnv ) in
     (*let contrainte,newEnv = where ( order_by_nb_var(q.const) , initEnv ) in*)
     let selection = select ( q.dist , newEnv ) in
     let forUnionEnv = {current=newEnv.current;suite=[]} in
     let contrainteunion,unionEnv = where ( no_order(union) , forUnionEnv ) in
     let selectionunion = select ( q.dist , unionEnv ) in
     let solution_modifiers = modifiers() in
     let finalquery = "val Qfinal = if (!("^listPaths(q.const)^")) {Array();} \n\t else{"^selection^".union("^selectionunion^")"^solution_modifiers^"}" in
     Printf.printf "%s \n" (preliminaires^contrainte^contrainteunion^finalquery)

;;

let () = 
  let l = read_lines Sys.argv.(1) in
  let rec aux l = match l with
    | [] -> Printf.printf ""
    | t :: q -> translator t; Printf.printf "\n"; aux q
  in aux l
;;

let test() =
  let ic = open_in Sys.argv.(1) in
  try 
      let line = input_line ic in  (* read line from in_channel and discard \n *)
      print_endline line;          (* write the result to stdout *)
      Printf.printf "\n";
      flush stdout;                (* write on the underlying device now *)
      close_in ic;                  (* close the input channel *) 
      
      translator(line)
  with e ->                      (* some unexpected exception occurs *)
    close_in_noerr ic;           (* emergency closing *)
    raise e                      (* exit with error: files are closed but
                                    channels are not flushed *)
;;
