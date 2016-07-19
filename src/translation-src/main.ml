open Struct;;    (* Types used. *)
open Parser;;    (* SPARQL parser. *)
open ToScalaUtils;;   (* To return the Scala commands. *)
open ToScalaOneFile;;   (* To return the Scala commands. *)
open ToScalaVertical;;   (* To return the Scala commands. *)


let () = 
  let vertical = Str.string_partial_match (Str.regexp "vertical") Sys.argv.(0) 0 in
  let l = read_lines Sys.argv.(if vertical then 1 else 2) in
  let rec aux l = match l with
    | [] -> Printf.printf ""
    | t :: q ->
        if vertical
        then translatorVertical t
        else translatorOneFile(t,Sys.argv.(1)); Printf.printf "\n"; aux q
        in aux l
;;
