open List;;
open Str;;
open Struct;;


(* A simple function waiting for String.trim with version 4.00.0 ... *)
let rec trim s =
  let l = String.length s in 
  if l=0 then s
  else if s.[0]=' ' || s.[0]='\t' || s.[0]='\n' || s.[0]='\r' then
    trim (String.sub s 1 (l-1))
  else if s.[l-1]=' ' || s.[l-1]='\t' || s.[l-1]='\n' || s.[l-1]='\r' then
    trim (String.sub s 0 (l-1))
  else
    s
;;
      
let string2atom (str:string):atom =
  let s = trim str in
  if s.[0] = '?' 
  then let w = String.sub s 1 ((String.length s)-1) in Variable( w )
  else Exact(s)
;;

let atom_map (l:string list):atom list = List.map string2atom l;;
(*let rec aux(l,result)=match l with
    | [] -> result
    | t :: q -> aux(q,(string2atom t)::result)
  in aux(l,[])
;;*)

let rec nextText (l:split_result list) (delim:string) =
  match l with
  | [] -> None
  | t::q -> match t with
    | Delim(del) -> if String.compare del delim = 0
      then let v=hd(q) in match v with | Text(titi) -> Some(titi) |_ -> None
      else nextText q delim
    | Text(tex) -> nextText q delim
;;

let makeTriples (l:string list) : triple list=
  let rec aux l result =
    match l with
    | [] -> rev(result)
    | t :: q -> let tr = split (regexp " ") t in 
		let triple = {subj=string2atom(hd(tr)) ; pred = string2atom(hd(tl(tr))) ; obj = string2atom(hd(tl(tl(tr))))} in
		aux q (triple :: result)
  in aux l []
;;

let parse (query:string) =
  let splitquery = full_split 
    (regexp "select\\|where\\|SELECT\\|WHERE\\|Select\\|Where\\|optional\\|union")
    (*"select ?x where ?x p1 ?y . ?y p2 ?z . ?z p3 ?x ." in*)
    query in
  let listOfDistinguished = nextText splitquery "select" in
  let listOfConstraints = nextText splitquery "where" in
  let listOfAdditional = nextText splitquery "union" in
  match listOfDistinguished,listOfConstraints,listOfAdditional with
  | Some(v),Some(c),add ->
    let listc = split (regexp "\\.") c in
    let listv = split (regexp ",") v in
    let listunion = match add with
      | None -> []
      | Some(union) -> split (regexp "\\.") union
    in
    {dist=atom_map(listv);const=makeTriples listc;union=makeTriples listunion}
  | _,_,_ -> failwith("Error in the SPARQL query format.\n")
;;


(*
  let rec aux(test) =
    match test with
    | [] -> Printf.printf("\n")
    | t :: q ->  match t with
      | Delim(del) -> aux(q)
      | Text(tex) -> Printf.printf"Useful stuff: %s, " tex ; aux q
  in aux(test) ;
*)
