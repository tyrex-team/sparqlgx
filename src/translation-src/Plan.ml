open Stat
open Algebra
let inf = max_int
let cost_shuffle = 4
let cost_broadcast = 4
let broadcast_threshold = 1000
let max_int32 = 2147483648*256

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

  
  let rec get_best_no_keys   = function
    | [] -> failwith ("Empty list to optimized @ "^__LOC__)
    | [id,a] ->
       [fst a, a, List.assoc id trad, []]
    | a::l ->
       let hash = List.fold_left (fun x y -> x+(fst y)) 0 (a::l) in
       begin 
         match dyn.(hash) with
         | None ->
            let res = test_all_split [] [a] [] l in
            dyn.(hash) <- Some res ; res
         | Some v -> v
       end

  and get_best (k:string list) (t:(int*'a combstat) list) =
    let rec foo = function
      | [] -> failwith ("Empty plan @ "^__LOC__)
      | [cost,stat,plan,key] ->
         if key = k || k = [] || key = []
         then (cost,stat,plan)
         else (cost+cost_shuffle*(fst stat),stat,plan)
      | a::q -> min (foo [a]) (foo q)
    in
    foo (get_best_no_keys t)

    
  and propose agg key cost stat plan =
    (* propose add (cost,stat,plan,key) to agg where we remove
      candidate plans that are useless i.e. plans with a cost greater
      than reshuffling another plan) *)
    let rec foo l = match l with
      | [] -> [cost,stat,plan,key]
      | (c2,s2,p2,k2)::q ->
         if key = k2
         then
           if cost > c2
           then l
           else (cost,stat,plan,key)::q
         else
           if c2+cost_shuffle*fst s2<cost || (key=[]&&c2<cost)
           then l
           else
             if cost+cost_shuffle*fst stat<c2|| (k2=[]&&c2>cost)
             then foo q
             else (c2,s2,p2,k2)::foo q
    in
    foo agg

  and test_broadcast agg stat smallset largeset =
    let cb,sb,pb = get_best [] largeset
    and ca,sa,pa = get_best [] smallset in

    propose agg [] (ca+cb+fst stat+fst sa*cost_broadcast) stat (JoinWithBroadcast(pb,pa))
   
  and test_all_split agg a b= function
    | [] -> if b <> []
            then
              let key_join = inter (get_col a) (get_col b) in
              let cb,sb,pb = get_best key_join b 
              and ca,sa,pa = get_best key_join a 
              in
              let s_res = combine sa sb in
              let c_res = cb+ca+fst s_res in
              let p_res = if fst sa > fst sb then Join(pa,pb) else Join(pb,pa)
              in
              if fst s_res > max_int32 then failwith "TOO BIG" ;
              if c_res > max_int32*1000 then failwith "TOO BIG" ;
              if fst s_res = 0
              then [0,s_res,Empty,[]]
              else
                let try_broad_agg =
                  if min (fst sa) (fst sb) < broadcast_threshold
                  then
                    if fst sa < fst sb
                    then test_broadcast agg s_res a b
                    else  test_broadcast agg s_res b a
                  else agg
                in
                propose try_broad_agg key_join c_res s_res p_res 
            else
              agg
    | x::t ->
       let agg1 = test_all_split agg (x::a) b t in
       test_all_split agg1 a (x::b) t
      
  in

  get_best [] tpcost




  
(* let t1 = get_tp_stat s2 (Variable("?X"),Exact "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",Exact ("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent>")) *)
(* let t2 = get_tp_stat s2 (Variable("?Y"),Exact "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",Exact ("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University>")) *)
(* let t3 = get_tp_stat s2 (Variable("?Z"),Exact "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",Exact ("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department>")) *)
(* let t4 = get_tp_stat s2 (Variable("?X"),Exact "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf>",Variable("?Z")) *)
(* let t5 = get_tp_stat s2 (Variable("?Z"),Exact "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf>",Variable("?Y")) *)
(* let t6 = get_tp_stat s2 (Variable("?X"),Exact "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom>",Variable("?Y")) *)

(* let c,s,p = get_optimal_plan_with_stat [1,t1; 2,t2; 4,t3; 8,t4; 16,t5; 32,t6 ] *)

(* let _ = listFromStat p  *)
