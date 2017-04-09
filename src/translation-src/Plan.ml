open Stat
open Algebra
open Big_int

let inf = max_int
let cost_shuffle = big_int_of_int 4
let cost_broadcast = big_int_of_int  4
let broadcast_threshold = big_int_of_int 1000

let get_optimal_plan_with_stat (tp_list:(algebra*'a combstat*string list) list) =

  let size, tp_id,tpcost, trad, tpcols =
    let rec foo = function
      | [] -> 0,[],[],[],[]
      | (b,a,c)::q ->
         let s,l0,l1,l2,l3 = foo q in
         s+1,(s::l0),((s,a)::l1),((s,b)::l2),((s,c)::l3)
    in
    foo tp_list
  in

  let p2 = Pervasives.(lsl) 1 in

  let size_p2 = p2 size in

  let rec get_hash = function
    | [] -> 0
    | a::q -> p2 a + get_hash q
  in

  
  let union l1 l2 =
    l1@(List.filter (fun x -> not( List.mem x l1)) l2)
  in

  let inter l1 l2 =
    List.filter (fun x -> List.mem x l2) l1
  in
   
  let dyn_col = Array.make size_p2 [] in 
  let rec get_col h l =
    assert (h<size_p2) ;
    assert (h>=0) ;
    match dyn_col.(h) with
    | [] ->
       let res = match l with
         | [] -> []
         | (a)::q -> union (List.assoc a tpcols) (get_col (h-p2 a) q)
       in
       dyn_col.(h) <- res ; res
    | v -> v
  in

  let dyn_conn = Array.make size_p2 [] in
  let partition h l =
    (* 
       add takes a set (x) of columns, an id of a TP (whose columns
       are x) and a list of paris of sets of columns and ids of TP.  add
       x id adds (x,[id]) to this list and merge sets with common columns
     *)
    let rec add cols ids = function
      | [] -> [cols,ids]
      | (o_cols,o_ids)::q ->
         if List.exists (fun col -> List.mem col o_cols) cols
         then add (union cols o_cols) (ids@o_ids) q
         else (o_cols,o_ids)::(add cols ids q)
    in
    let rec foo h l =
      assert (h<size_p2) ;
      match dyn_conn.(h) with
      | [] ->
         let res = match l with
           | [] -> []
           | (a)::q -> add (List.assoc a tpcols) [a] (foo (h-p2 a) q)
         in
         dyn_conn.(h) <- res ; res
      | v -> v
      
    in
    foo h l
  in

  let is_connected h l =
    match partition h l with
    | [] -> failwith __LOC__
    | [a] -> true
    | l -> false
  in

  let is_star l =
    [] <> (List.map (fun x -> List.assoc x tpcols) l |>
             List.fold_left inter [])
  in
  
  let dyn_best = Array.make size_p2 None in
  let rec get_best_no_keys hash l = match l with
    | [] -> failwith ("Empty list to optimized @ "^__LOC__)
    | [id] ->
       let size = fst (List.assoc id tpcost) in
       [size, size, List.assoc id trad, []]
    | a::q ->
       begin
         assert (hash<size_p2) ;
         match dyn_best.(hash) with
         | None ->
            let res = match partition hash l with
              | [] -> failwith __LOC__
              | [_] ->
                 (* Single component *)
                 let s_res = compute_stat hash l in
                 test_all_split s_res [] ([a],p2 a) ([],0) q                
              | (cols,ids)::q ->
                 let ids_q = snd (List.split q) |> List.fold_left (@) [] in
                 let c1,s1,p1 = get_best (get_hash ids) [] ids in
                 let c2,s2,p2 = get_best (get_hash ids_q) [] ids_q in
                 let s_res = (mult_big_int s1 s2) in
                 [add_big_int (add_big_int c1 c2) s_res,s_res,Join(p1,p2),[]]
            in
            dyn_best.(hash) <- Some res ; res
         | Some v -> v
       end

  and dyn_stat = Array.make size_p2 None
  and compute_stat hash l =
    let rec foo hash l =
      assert (hash<size_p2) ;
      match dyn_stat.(hash) with
      | None ->
         let r = 
           match l with 
           | [] -> failwith __LOC__
           | [a] -> List.assoc a tpcost
           | a::q -> combine (foo (hash-p2 a) q) (foo a [a])        
         in
       dyn_stat.(hash) <- Some r ; r
      | Some v -> v
    in
    fst (foo hash l)
            
  and min_prop (c1,s1,p1) (c2,s2,p2) =
    if lt_big_int c1 c2
    then (c1,s1,p1)
    else (c2,s2,p2)
       
  and get_best hash (k:string list) (t:int list)  =
    let rec foo = function
      | [] -> failwith ("Empty plan @ "^__LOC__)
      | [cost,size,plan,key] ->
         if key = k || k = [] || key = []
         then (cost,size,plan)
         else (add_big_int cost (mult_big_int cost_shuffle size),size,plan)
      | a::q -> min_prop (foo [a]) (foo q)
    in
    foo (get_best_no_keys hash t)

    
  and propose agg key cost (size:bi) plan =
    (* propose add (cost,stat,plan,key) to agg where we remove
      candidate plans that are useless i.e. plans with a cost greater
      than reshuffling another plan) *)
    let rec foo l = match l with
      | [] -> [cost,size,plan,key]
      | (c2,s2,p2,k2)::q ->
         if key = k2
         then
           if lt_big_int c2 cost
           then l
           else (cost,size,plan,key)::q
         else
           if lt_big_int (add_big_int c2 (mult_big_int cost_shuffle s2)) cost || (key=[]&&lt_big_int c2 cost)
           then l
           else
             if lt_big_int (add_big_int cost (mult_big_int cost_shuffle size)) c2 || (k2=[]&&lt_big_int cost c2)
             then foo q
             else (c2,s2,p2,k2)::foo q
    in
    foo agg

  and test_broadcast agg size (smallset,hasha) (largeset,hashb) =
    let cb,sb,pb = get_best hashb [] largeset
    and ca,sa,pa = get_best hasha [] smallset in

    let cost_of_broadcast = mult_big_int sa cost_broadcast in
    let cost_children = add_big_int ca cb in
    let cost_materialization = add_big_int cost_children cost_of_broadcast  in
    let cost_tot = add_big_int cost_materialization size in
    propose agg [] cost_tot size (JoinWithBroadcast(pb,pa))
   
  and test_all_split s_res agg (a,ha) (b,hb)= function
    | [] -> if b <> []
            then
              let key_join = inter (get_col ha a) (get_col hb b) in
              let cb,sb,pb = get_best hb key_join b 
              and ca,sa,pa = get_best ha key_join a 
              in
              let c_res = add_big_int (add_big_int cb ca) s_res in
              let p_res = if lt_big_int sa sb then Join(pa,pb) else Join(pb,pa)
              in
              let try_broad_agg =
                if lt_big_int (min_big_int sa sb) broadcast_threshold
                then
                  if lt_big_int sa sb
                  then test_broadcast agg s_res (a,ha) (b,hb)
                  else  test_broadcast agg s_res (b,hb) (a,ha)
                else agg
              in
              propose try_broad_agg key_join c_res s_res p_res 
            else
              agg
    | x::t ->
       let agg1 = test_all_split s_res agg (x::a,ha+p2 x) (b,hb) t in
       test_all_split s_res agg1 (a,ha) (x::b,hb+p2 x) t
      
  in
  get_best (size_p2-1) [] tp_id



  
(* let t1 = get_tp_stat s2 (Variable("?X"),Exact "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",Exact ("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent>")) *)
(* let t2 = get_tp_stat s2 (Variable("?Y"),Exact "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",Exact ("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University>")) *)
(* let t3 = get_tp_stat s2 (Variable("?Z"),Exact "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",Exact ("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department>")) *)
(* let t4 = get_tp_stat s2 (Variable("?X"),Exact "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf>",Variable("?Z")) *)
(* let t5 = get_tp_stat s2 (Variable("?Z"),Exact "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf>",Variable("?Y")) *)
(* let t6 = get_tp_stat s2 (Variable("?X"),Exact "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom>",Variable("?Y")) *)

(* let c,s,p = get_optimal_plan_with_stat [1,t1; 2,t2; 4,t3; 8,t4; 16,t5; 32,t6 ] *)

(* let _ = listFromStat p  *)
