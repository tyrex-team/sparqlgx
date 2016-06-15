open Struct;;

(* The COST functions used to sort the list of constraints *)


(* This cost doesn't change anything => initial version *)
let no_order (tp:triple list):triple list =
  tp
;;

(* Sort triples by number of variables: one-variable triples first. *)
let order_by_nb_var (tp: triple list):triple list =
  let isVar (a:atom) = match a with | Variable(_) -> 1 | Exact(_) -> 0 in
  let nbVar (t:triple) = isVar(t.subj)+isVar(t.pred)+isVar(t.obj) in
  let rec aux l r1 r2 = match l with
    | [] -> (List.rev r1)@(List.rev r2)
    | t :: q -> if nbVar t = 1
      then aux q (t :: r1) r2
      else aux q r1 (t :: r2)
  in aux tp [] []
;;

let cost (choice:int) (tp:triple list):triple list = 
  match choice with
  | 0 -> no_order tp (*No optim'*)
  | 1 -> order_by_nb_var tp (*Simple sort for optim'*)
  | _ -> no_order tp (*Default value: no optimization.*)
;;
