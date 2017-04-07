 open Sparql
open Scanf
open Algebra
open Plan   

let stats = Hashtbl.create 17

(* let () = Hashtbl.add stats ("s","*") 1 ; Hashtbl.add stats ("p","*") 1 ; Hashtbl.add stats ("o","*") 1 *)
let load_into filename hshtbl = 
  (try
    let chan = Scanning.from_file filename in
    
    let rec foo name nb =
      for i = 0 to nb-1 do
        Scanf.bscanf chan "%s %d\n" (fun i j -> Hashtbl.add hshtbl (name,i) j) ;
      done
    in
    try
      Scanf.bscanf chan "%d %d %d\n" (fun i j k -> foo "s" i ; foo "p" j ; foo "o" k) ;
    with | End_of_file -> Scanning.close_in chan
   with | Sys_error s -> failwith ("Stat file problem, "^s))
;;

let load filename =
  load_into filename stats
(* Hashtbl.iter (fun (x,y) z -> print_string ("("^x^","^y^") "^(string_of_int z)^"\n")) stats *)

;;

let rec comb = function
  | [] -> failwith "TP error"
  | [a] -> a
  | a::q -> Join(a,comb q)
;;
  
let no_optim trad_tp l =
  comb (List.map trad_tp l)
;;
let graded_no_cartesian trad_tp =
  let rec combine acc = function
    | [] -> comb (List.map snd acc)
    | (grade_tp,(s,p,o))::q ->
       let rec do_join c_term c_cols = function
         | [] -> [c_cols,c_term]
         | (o_cols,o_term)::q ->
            if List.exists (fun x -> List.mem x c_cols) o_cols
            then do_join (Join(c_term,o_term)) (c_cols@o_cols) q
            else (o_cols,o_term)::(do_join c_term c_cols q)
       in
       combine (do_join (trad_tp (s,p,o)) (list_var [s;p;o]) acc) q
  in
  combine []
;;
  
let no_cartesian trad_tp l  = 
  graded_no_cartesian trad_tp (List.map (fun x -> (),x) l)
;;

let reorder trad_tp l =  
             
  let rec nb_var (s,p,o) =
    List.fold_left (fun ac el -> match el with Variable _ -> 1 +ac | _ -> ac) 0 [s;p;o]
  in

  let sel name value =
    try 
      Hashtbl.find stats (name,value)
    with Not_found ->
      try 
        Hashtbl.find stats (name,"*")
      with Not_found -> -1
  in
  
  let grade (s,p,o) =
    let rec foo = function
    | [] -> None
    | (Variable _,n)::q -> foo q
    | (Exact v,n)::q ->
       let s = sel n v in
       Some (match foo q with
             | None -> s
             |  Some v -> min v s
            )
    in

    match foo [s,"s";p,"p";o,"o"] with
    | Some g -> (nb_var (s,p,o),g),(s,p,o)
    | None -> (0,0),(s,p,o)
  in

  let rec sort =
    List.sort (fun x y -> if x>y then 1 else -1)
  in
  
  List.map grade l |>
    sort |>
    graded_no_cartesian trad_tp
  

;;

let stat_file = ref None
              
let load_full_stat s =
  stat_file := Some (Stat.fullstat s) 
  
let full_stat trad_tp l =

  let get_var (a,b,c) =
    let rec foo = function
      | [] -> []
      | Exact _ :: q -> foo q
      | Variable v :: q -> v::foo q
    in
    foo [a;b;c]
  in
  
  match !stat_file with
  | None -> failwith "Full stat not loaded!"
  | Some s ->
     let tp = List.map (fun x ->trad_tp x,Stat.get_tp_stat s x, get_var x) l in
     let cost, stat, plan = get_optimal_plan_with_stat tp in
     plan
      
