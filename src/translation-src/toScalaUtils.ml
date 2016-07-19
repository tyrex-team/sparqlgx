open Struct;;

(* The function to translate the solution modifiers. *)
let modifiers () = ".collect;\n";;


(***************************
 * Operations on Environment
 * - have data about a variable isInEnv
 * - nb of join => collateral of isInEnv
 * - returns the select command selectFromEnv
 * - updates with a variable upEnv
****************************)

(* Returns the element of the variable if it exists *)
let isInEnv ( (x:string) , (env:environment) ) =
  let rec aux(s,e) = match e with
    | [] -> None
    | t :: q -> if String.compare s t.var = 0
      then Some(t)
      else aux(s,q)
  in aux(x,env.suite)
;;

(* This funciton does 2 things:
   1. Add x at the correct place (where the former x was) in the list in env
   2. Change the .label and the .varlist of the other variables that are impacted by 'x'*)
let upEnv (x:element) (env:environment) =
  let rec aux(x,e,result) = match e with
    | [] -> x :: result
    | t :: q -> if String.compare x.var t.var = 0
      then aux(x,q,x :: result) (* We add the new version of 'x'. *)
      else begin 
	if List.mem t.var x.varlist
	then let newt = {var=t.var;label=x.label;nbJoin=t.nbJoin;varlist=x.varlist} in
	     aux(x,q,newt :: result) (* We change the version of 't'. *)
	else aux(x,q,t :: result) (* We just keep scanning. *)
      end
  in 
  let newsuite = aux(x,env.suite,[]) in
  {current=env.current;suite=newsuite}
;;



(* The idea of 'adjust' is to provide a modified 'l1' where duplicates with 'l2' are a bit changed
   e.g. l1=[a;b;c] l2=[a;x;y] adjust(l1,l2)=[a1;b;c] *)
   
let adjust (l1:string list) (l2:string list):string list =
 (* Obviously, a problem can occured if a variable has been named ?xbis also... *)
 List.map (fun t -> if List.mem t l2 then t^"bis" else t) l1 
;;

let printlist(l:string list):string = 
  let rec aux(l) = match l with
  | [] -> ""
  | [t] -> t
  | t :: q -> t^","^aux(q)
  in "("^aux(l)^")"
;;

let union((l1:string list),(l2:string list)):string list=
  let rec deleteDuplicate ll = (* A stupid version. *)
    match ll with 
    | [] -> []
    | x :: [] -> x :: [] 
    | x :: y :: rest -> 
      if compare x y = 0
      then deleteDuplicate (y :: rest) 
      else x :: deleteDuplicate (y :: rest) 
  in
  let l1sort = List.sort compare l1 in
  let l2sort = List.sort compare l2 in
  deleteDuplicate(List.merge compare l1sort l2sort)
;;


let return_biggest_label(env:environment):int=
 let initial:element={var="";label=0;nbJoin=0;varlist=[]} in
 let best =
   List.fold_left (fun ac el -> if List.length el.varlist >= List.length ac.varlist then el else ac)  initial env.suite
 in best.label
;;


let contained l1 l2 = (* We assuming that there are no duplicate values in l1 and l2 *)
  List.for_all (fun x -> List.mem x l2) l1
;;

(* Return the element that having a varlist containing dist and having the biggest label *)
let return_good((dist:string list) , (env:environment) ):element =
  let initial:element={var="";label=0;nbJoin=0;varlist=[]} in
  List.fold_left (fun ac el -> if el.label >= ac.label && contained dist el.varlist then el else ac)  initial env.suite

;;

(* The function to translate the distingushed variables. *)
let select ( (dist:atom list) , (env:environment) ) =
  if List.hd dist = Exact("*")
  then
    let good_label:int = return_biggest_label(env) in
    "Q"^string_of_int(good_label)
  else
    let string_dist = List.map atom2string dist in
    let good_element:element = return_good(string_dist,env) in
    "Q"^string_of_int(good_element.label)^".map{case "^(printlist good_element.varlist)^"=>"^(printlist(string_dist))^"}"
;;

(* Lexing  *)


let read_lines name : string list =
  let ic = open_in name in
  let try_read () =
    try Some (input_line ic) with End_of_file -> None in
  let rec loop acc = match try_read () with
    | Some s -> loop (s :: acc)
    | None -> close_in ic; List.rev acc in
  loop []
;;
