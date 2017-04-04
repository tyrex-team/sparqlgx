open Stat
open Algebra
let inf = max_int
          
let get_optimal_plan_with_stat (tp_list:(algebra*'a combstat*string list) list) =

  let size_p2, tpcost, trad, tpcols =
    let rec foo = function
      | [] -> 1,[],[],[]
      | (b,a,c)::q ->
         let s,l1,l2,l3 = foo q in
         s*2,((s,a)::l1),((s,b)::l2),((s,c)::l3)
    in
    foo tp_list
  in
      
  let dyn = Array.make size_p2 None in

  let dyn_col = Array.make size_p2 [] in 

  let union l1 l2 =
    l1@(List.filter (fun x -> not( List.mem x l1)) l2)
  in

  let inter l1 l2 =
    List.filter (fun x -> List.mem x l2) l1
  in
  
  let rec get_col l =
    let hash = List.fold_left (fun x y-> x+fst y) 0 l in
    match dyn_col.(hash) with
    | [] ->
       let res = match l with
         | [] -> []
         | (a,_)::q -> union (List.assoc a tpcols) (get_col q)
       in
       dyn_col.(hash) <- res ; res
    | v -> v
  in
  
  let rec get_best = function
    | [] -> failwith ("Empty list to optimized @ "^__LOC__)
    | [id,a] ->
       fst a, a, List.assoc id trad, []
    | a::l ->
       let hash = List.fold_left (fun x y -> x+(fst y)) 0 (a::l) in
       begin 
         match dyn.(hash) with
         | None ->
            let res = test_all_split [a] [] l in
            dyn.(hash) <- Some res ; res
         | Some v -> v
       end
  and test_all_split a b = function
    | [] -> if b <> []
            then
              let cb,sb,pb,kb = get_best b
              and ca,sa,pa,ka = get_best a
              and key_join = inter (get_col a) (get_col b)
              in
              let s_res = combine sa sb in
              let c_res = cb+ca+fst s_res+(if kb<>key_join then 10*fst sb else 0)+(if ka<>key_join then 10*fst sa else 0) in
              let p_res = if fst sa > fst sb then Join(pa,pb) else Join(pb,pa) in
              c_res,s_res,p_res, key_join
            else
              inf,(inf,[]),Readfile3,[]
    | x::t ->
       min (test_all_split (x::a) b t) (test_all_split a (x::b) t)
      
  in

  get_best tpcost

(* let t1 = get_tp_stat s2 (Variable("?X"),Exact "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",Exact ("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent>")) *)
(* let t2 = get_tp_stat s2 (Variable("?Y"),Exact "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",Exact ("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University>")) *)
(* let t3 = get_tp_stat s2 (Variable("?Z"),Exact "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",Exact ("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department>")) *)
(* let t4 = get_tp_stat s2 (Variable("?X"),Exact "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf>",Variable("?Z")) *)
(* let t5 = get_tp_stat s2 (Variable("?Z"),Exact "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf>",Variable("?Y")) *)
(* let t6 = get_tp_stat s2 (Variable("?X"),Exact "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom>",Variable("?Y")) *)

(* let c,s,p = get_optimal_plan_with_stat [1,t1; 2,t2; 4,t3; 8,t4; 16,t5; 32,t6 ] *)

(* let _ = listFromStat p  *)
