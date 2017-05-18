(*

This file defines a data-structure "tree_mul" that allows for the fast
computation of the total(s1,s2,x,k) for x and k variable.

Node_mul(a,b,m1,m2) describe a tree with a elements before the first
element describe by m2 and the minimum multiplier of m1 is b.

 *)

open Big_int
type bi = Big_int.big_int

type tree_mul =
  | Node_mul of bi*bi*tree_mul*tree_mul
  | Leaf_mul of bi*bi*bi

              
let count (tbl,nbDef,nbPerDef,totalDef) (v:'a) =
  try
    Hashtbl.find tbl v
  with Not_found -> nbPerDef
    

(* Add a parameter to introduce a maximal multiplier!*)

let build_tree_mul mul =
  let rec foo minMultBefore nbBefore totBefore mul = match mul with
    | [] -> failwith __LOC__
    | [a,b] -> Leaf_mul(a,b,sub_big_int totBefore (mult_big_int nbBefore a)),
               min_big_int minMultBefore b,
               add_big_int totBefore (mult_big_int a b),
               add_big_int nbBefore b
    | l ->
       let rec split beg  = function
         | 0,l -> List.rev beg,l
         | n,a::q ->split (a::beg) (n-1,q)
         | _ -> failwith __LOC__
       in
       let l1, l2 = split [] (List.length l/2,l) in
       let t1,minMultBefore1,totBefore1,nbBefore1 = foo minMultBefore nbBefore totBefore l1 in
       let t2,minMultBefore2,totBefore2,nbBefore2 = foo minMultBefore1 nbBefore1 totBefore1 l2 in
       Node_mul(nbBefore1,minMultBefore1,t1,t2),minMultBefore2,totBefore2,nbBefore2
  in
  match mul with
  | [] -> Leaf_mul(zero_big_int,zero_big_int,zero_big_int)
  | mul -> 
     let t1,minMultBefore,tot,nbBefore = foo zero_big_int zero_big_int zero_big_int mul in
     Node_mul(nbBefore,minMultBefore,t1,Leaf_mul(zero_big_int,zero_big_int,tot))
       
  

let rec simplify_mul = function
  | [] -> []
  | (a,b)::q when sign_big_int a = 0 -> []
  | (a,b)::q when sign_big_int b = 0 -> simplify_mul q
  | (a,b)::(c,d)::q when eq_big_int a c -> simplify_mul ((a,add_big_int b d)::q)
  | a::q -> a::simplify_mul q

    
let compute_mul (t1,s1) (t2,s2) =

  
  let compute_mul_col s1 s2 =
    let (tbl2,nbDef2,nbPerDef2,totalDef2) = s2 in
    let (tbl1,nbDef1,nbPerDef1,totalDef1) = s1 in
    let t1_special = t1 in
    (* we should minus common with t2*)
    Hashtbl.fold (fun v n ac -> (n,count s1 v)::ac) tbl2 [(nbPerDef2,t1_special)] |>
      List.sort (fun (a,x) (b,y) -> compare_big_int b a)  |>
      simplify_mul 
  in

  let  rec combine_mul m1 m2 = match (m1,m2) with
    | [],_ | _,[] -> []
    | (a,b)::q, (c,d)::t ->
       if lt_big_int b d
       then (min_big_int a c,b)::combine_mul q ((c,sub_big_int d b)::t)
       else (min_big_int a c,d)::combine_mul ((a,sub_big_int b d)::q) t       
  in


  ListSet.inter (List.map fst s1) (List.map fst s2) |>  
    List.fold_left (fun ac c -> combine_mul ac (compute_mul_col (List.assoc c s1) (List.assoc c s2)))  [t2,t1]  |>
    simplify_mul |>
    build_tree_mul


let rec mult mul n = match mul with
  | Leaf_mul(a,b,base) -> add_big_int base (mult_big_int a n)
  | Node_mul(v,_,m1,m2) -> if lt_big_int v n then mult m2 n else mult m1 n
                       
let rec total mul n k =


  (* 

     remove mul nbBefore finds the first l=leaf(a,b,base) with b>k
     then either the nbBefore (i.e. the number of elements before l)
     is greater than n (and mult mul n is useless since total mul n k
     = k×n) or it is lower and we need to remove base (for nbBefore
     elements with a mult>K) and add back k×nbBefore

   *)
  let rec remove  mul nbBefore = match mul with
    | Leaf_mul(a,b,base) ->       
       if lt_big_int nbBefore n
       then sub_big_int base (mult_big_int nbBefore k)
       else zero_big_int 
    | Node_mul(nb,k_min1,m1,m2) ->
       if lt_big_int k_min1 k
       then remove m2 nb
       else remove m1 nbBefore
  in


  min_big_int
    (mult_big_int n k)
    (sub_big_int (mult mul n) (remove mul zero_big_int))

