open Stat_combine
open Algebra
open Big_int

let inf = max_int
let cost_shuffle = big_int_of_int 4
let cost_broadcast = big_int_of_int 60
let cost_cartesian = big_int_of_int  10000
let broadcast_threshold = big_int_of_int 1000000
let cost_broadcast_unit = big_int_of_int 1000000
let stat_of_plan = Hashtbl.create 17
                 
let get_size p = try int_of_big_int (fst (Hashtbl.find stat_of_plan p)) with Not_found -> (-1)


let get_optimal_plan_with_stat (tp_list:(algebra*'a combstat*string list) list) =

  let size = List.length tp_list in
  let tpcost = Array.init size (fun i -> match List.nth tp_list i with (a,b,c) -> b) in
  let trad = Array.init size (fun i -> match List.nth tp_list i with (a,b,c) -> a) in
  let tpcols = Array.init size (fun i -> match List.nth tp_list i with (a,b,c) -> c) in
  let tp_id = List.mapi (fun i a -> i) tp_list  in

  let p2 = Pervasives.(lsl) 1 in

  let size_p2 = p2 size in

  let rec get_hash = function
    | [] -> 0
    | a::q -> p2 a + get_hash q
  in

     
  let dyn_col = Array.make size_p2 [] in 
  let dyn_conn = Array.make size_p2 [] in
  let dyn_best = Array.make size_p2 None in
  let dyn_star = Array.make size_p2 None in

  let rec get_col h l =
    assert (h<size_p2) ;
    assert (h>=0) ;
    match dyn_col.(h) with
    | [] ->
       let res = match l with
         | [] -> []
         | (a)::q -> ListSet.union (tpcols.(a)) (get_col (h-p2 a) q)
       in
       dyn_col.(h) <- res ; res
    | v -> v
  in

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
         then add (ListSet.union cols o_cols) (ids@o_ids) q
         else (o_cols,o_ids)::(add cols ids q)
    in
    let rec foo h l =
      assert (h<size_p2) ;
      match dyn_conn.(h) with
      | [] ->
         let res = match l with
           | [] -> []
           | (a)::q -> add (tpcols.(a)) [a] (foo (h-p2 a) q)
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

  let is_star = function
    | [] -> false
    | a::q ->
       [] <> (List.map (fun x -> tpcols.(x)) q |>
             List.fold_left ListSet.inter tpcols.(a))
  in

  let size_of_stat (s:'a combstat) = match s with (a,_) -> a in
  
  let rec get_best_no_keys hash l = match l with
    | [] -> failwith ("Empty list to optimized @ "^__LOC__)
    | [id] ->
       let size = fst (tpcost.(id)) in
       [size, tpcost.(id), trad.(id), []]
    | a::q ->
       begin
         assert (hash<size_p2) ;
         match dyn_best.(hash) with
         | None ->
            let res = match partition hash l with
              | [] -> failwith __LOC__
              | [_] ->
                 (* Single component *)
                 if is_star l
                 then plan_for_star l
                 else fst (test_all_connected_split [] None ([a],p2 a) ([],0) q)
              | (cols,ids)::q ->
                 let ids_q = snd (List.split q) |> List.fold_left (@) [] in
                 let c1,s1,p1 = get_best (get_hash ids) [] ids in
                 let c2,s2,p2 = get_best (get_hash ids_q) [] ids_q in
                 let s_res = combine s1 s2 in
                 let p_res = Join(p1,p2) in
                 Hashtbl.add stat_of_plan p_res s_res ;
                 [add_big_int (add_big_int c1 c2) (mult_big_int (size_of_stat s_res) cost_cartesian),s_res,Join(p1,p2),[]]
            in
            dyn_best.(hash) <- Some res ; res
         | Some v -> v
       end
            
  and min_prop (c1,s1,p1) (c2,s2,p2) =
    if lt_big_int c1 c2
    then (c1,s1,p1)
    else (c2,s2,p2)
       
  and get_best hash (k:string list) (t:int list)  =
    let rec foo = function
      | [] -> failwith ("Empty plan @ "^__LOC__)
      | [cost,stat,plan,key] ->
         if key = k || k = [] || key = []
         then (cost,stat,plan)
         else (add_big_int cost (mult_big_int cost_shuffle (size_of_stat stat)),stat,plan)
      | a::q -> min_prop (foo [a]) (foo q)
    in
    foo (get_best_no_keys hash t)
    
    
  and propose agg key cost (stat:'a combstat) plan =
    (* propose add (cost,stat,plan,key) to agg where we remove
      candidate plans that are useless i.e. plans with a cost greater
      than reshuffling another plan) *)
    let rec foo l = match l with
      | [] -> [cost,stat,plan,key]
      | (c2,s2,p2,k2)::q ->
         if key = k2
         then
           if lt_big_int c2 cost
           then l
           else (cost,stat,plan,key)::q
         else
           if lt_big_int (add_big_int c2 (mult_big_int cost_shuffle (size_of_stat s2))) cost || (key=[]&&lt_big_int c2 cost)
           then l
           else
             if lt_big_int (add_big_int cost (mult_big_int cost_shuffle (size_of_stat stat))) c2 || (k2=[]&&lt_big_int cost c2)
             then foo q
             else (c2,s2,p2,k2)::foo q
    in
    foo agg

  and test_broadcast agg stat (smallset,hasha) (largeset,hashb) =
    let cb,sb,pb = get_best hashb [] largeset
    and ca,sa,pa = get_best hasha [] smallset in

    let cost_of_broadcast = add_big_int cost_broadcast_unit (mult_big_int (size_of_stat sa) cost_broadcast) in
    let cost_children = add_big_int ca cb in
    let cost_materialization = add_big_int cost_children cost_of_broadcast  in
    let cost_tot = add_big_int cost_materialization (size_of_stat stat) in
    Hashtbl.add stat_of_plan (JoinWithBroadcast(pb,pa)) stat ;
    propose agg [] cost_tot stat (JoinWithBroadcast(pb,pa))
    
  and test_all_connected_split agg s_res (a,ha) (b,hb)= function
    | [] -> if b <> [] && is_connected ha a && is_connected hb b
            then
              let key_join = ListSet.inter (get_col ha a) (get_col hb b) in
              let cb,sb,pb = get_best hb key_join b 
              and ca,sa,pa = get_best ha key_join a 
              in
              let size_a = (size_of_stat sa) in
              let size_b = (size_of_stat sb) in
              
              let s_res = match s_res with | None -> combine sa sb | Some v -> v in
              let c_res = add_big_int (add_big_int cb ca) (size_of_stat s_res) in
              let p_res = if lt_big_int size_a size_b then Join(pa,pb) else Join(pb,pa)
              in
              Hashtbl.add stat_of_plan p_res s_res ;
              if sign_big_int (size_of_stat s_res) = 0
              then
                [zero_big_int,empty_stat (get_col (ha+hb) (a@b)),Empty,[]],Some s_res
              else
                let try_broad_agg =
                  if lt_big_int (min_big_int size_a size_b) broadcast_threshold
                  then
                    if lt_big_int size_a size_b
                  then test_broadcast agg s_res (a,ha) (b,hb)
                    else  test_broadcast agg s_res (b,hb) (a,ha)
                  else agg
                in
                propose try_broad_agg key_join c_res s_res p_res,Some s_res
            else
              agg,s_res
    | x::t ->
       let agg1,s_res = test_all_connected_split agg  s_res (x::a,ha+p2 x) (b,hb) t in
       test_all_connected_split agg1 s_res  (a,ha) (x::b,hb+p2 x) t

  and plan_for_star l =
    let rec foo h = function
      | [] -> 
         zero_big_int,empty_stat [],Empty,[]
      | [a] ->                    
         size_of_stat tpcost.(a),tpcost.(a),trad.(a),tpcols.(a)
      | id_1::id_2::id_3::x::q  ->
         begin 
           match dyn_star.(h) with
           | Some v -> v
           | None ->
              let (cost,stat,plan,cols) = foo (h-(p2 id_1)-(p2 id_2)-(p2 id_3)) (x::q) in
              let cost_res =
                add_big_int cost (
                  add_big_int
                    (add_big_int (size_of_stat tpcost.(id_3))
                       size_of_stat stat)
                    (add_big_int (size_of_stat tpcost.(id_1))
                       (size_of_stat tpcost.(id_2))))
              in
              let stat_res = combine (combine stat tpcost.(id_3)) (combine tpcost.(id_1) tpcost.(id_2)) in
              let plan_res = StarJoin4(plan,trad.(id_1),trad.(id_2),trad.(id_3)) in
              let cols_res = ListSet.inter cols tpcols.(id_1) in
              Hashtbl.add stat_of_plan plan_res stat_res ;
              dyn_star.(h) <- Some (cost_res,stat_res,plan_res,cols_res) ;
              if sign_big_int (size_of_stat stat_res) = 0
              then zero_big_int, stat_res, Empty, []
              else cost_res,stat_res,plan_res,cols_res
         end
      | id_1::id_2::x::q  ->
         begin 
           match dyn_star.(h) with
           | Some v -> v
           | None ->
              let (cost,stat,plan,cols) = foo (h-(p2 id_1)-(p2 id_2)) (x::q) in
              let cost_res = add_big_int cost (
                                           add_big_int (size_of_stat stat)
                                                       (add_big_int (size_of_stat tpcost.(id_1))
                                                                    (size_of_stat tpcost.(id_2))))
              in
              let stat_res = combine stat (combine tpcost.(id_1) tpcost.(id_2)) in
              let plan_res = StarJoin3(plan,trad.(id_1),trad.(id_2)) in
              let cols_res = ListSet.inter cols tpcols.(id_1) in
              Hashtbl.add stat_of_plan plan_res stat_res ;
              dyn_star.(h) <- Some (cost_res,stat_res,plan_res,cols_res) ;
              if sign_big_int (size_of_stat stat_res) = 0
              then zero_big_int, stat_res, Empty, []
              else cost_res,stat_res,plan_res,cols_res
         end
      | id::q ->
         match dyn_star.(h) with
         | Some v -> v
         | None ->
            let (cost,stat,plan,cols) = foo (h-p2 id) q in
            let cost_res = add_big_int cost (add_big_int (size_of_stat stat) (size_of_stat tpcost.(id))) in
            let stat_res = combine stat tpcost.(id) in
            let plan_res = Join(plan,trad.(id)) in
            let cols_res = ListSet.inter cols tpcols.(id) in
            Hashtbl.add stat_of_plan plan_res stat_res ;
            dyn_star.(h) <- Some (cost_res,stat_res,plan_res,cols_res) ;
            if sign_big_int (size_of_stat stat_res) = 0
            then zero_big_int, stat_res, Empty, []
            else cost_res,stat_res,plan_res,cols_res
    in
    match  
      List.sort (fun x y -> compare_big_int (size_of_stat tpcost.(x)) (size_of_stat tpcost.(y))) l 
    with
    | [] -> failwith __LOC__
    | a::q ->
       (* let center_of_star = (List.map (fun x -> tpcols.(x)) q |> List.fold_left inter tpcols.(a)) in *)
       [foo (get_hash (a::q)) (List.rev (a::q))]
  in

  let small_tps = tp_id |> List.filter (fun x -> 1=List.length tpcols.(x) && lt_big_int (fst tpcost.(x)) broadcast_threshold) in
  let large_tps = ListSet.minus tp_id small_tps in
  
  let rec filter_broadcast cur small_tps tps = match small_tps with
    | [] -> get_best (get_hash tps) [] tps
    | a::q ->
       let col = match tpcols.(a) with [x] -> x | _ -> failwith __LOC__ in
       let rec foo with_col without_col = function
         | [] -> with_col,without_col
         | x::t -> if tpcols.(x)=tpcols.(a)
                   then foo (x::with_col) without_col t
                   else foo with_col (x::without_col) t
       in
       let col_filter,others = foo [] [] q in
       let c2,s2,p2 = filter_broadcast cur others (a::col_filter@tps) in
       let combined_col_stat = List.fold_left (fun ac x -> combine tpcost.(x) ac) tpcost.(a) col_filter in
       let combined_term = List.fold_left (fun ac x -> Join(trad.(x),ac)) trad.(a) col_filter in
       
       let old_trad = Array.copy trad in
       let old_tpcost = Array.copy tpcost in
       
       let changed = 
         tps |>          
           List.filter (fun x -> List.mem col tpcols.(x)) |>
           List.fold_left ( fun ac i ->
                            trad.(i) <- FilterWithBroadcast(trad.(i),cur,[col]) ;
                            tpcost.(i) <- combine tpcost.(i) combined_col_stat ;
                            true 
                          ) false
       in
       if changed then
         begin 
           for i = 0 to size_p2 -1 do
             dyn_best.(i) <- None ;
             dyn_star.(i) <- None ;
           done ;
           let c,s,p = filter_broadcast (cur+1) others tps in
           let cost = add_big_int c cost_broadcast_unit in
           for i = 0 to Array.length tpcost -1 do
             tpcost.(i) <- old_tpcost.(i) ;
             trad.(i) <- old_trad.(i)
           done ;
           match p with
           | Empty -> zero_big_int,empty_stat (get_col (get_hash (a::tps)) (a::tps)),Empty
           | p ->
              if lt_big_int cost c2 then cost,s,Broadcast(cur,combined_term,p) else c2,s2,p2
         end
       else
         c2,s2,p2
                 
  in
  if large_tps <> []
  then
    filter_broadcast 0 small_tps large_tps
  else
    get_best (get_hash tp_id) [] tp_id



  
(* let t1 = get_tp_stat s2 (Variable("?X"),Exact "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",Exact ("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent>")) *)
(* let t2 = get_tp_stat s2 (Variable("?Y"),Exact "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",Exact ("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University>")) *)
(* let t3 = get_tp_stat s2 (Variable("?Z"),Exact "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",Exact ("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department>")) *)
(* let t4 = get_tp_stat s2 (Variable("?X"),Exact "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf>",Variable("?Z")) *)
(* let t5 = get_tp_stat s2 (Variable("?Z"),Exact "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf>",Variable("?Y")) *)
(* let t6 = get_tp_stat s2 (Variable("?X"),Exact "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom>",Variable("?Y")) *)

(* let c,s,p = get_optimal_plan_with_stat [1,t1; 2,t2; 4,t3; 8,t4; 16,t5; 32,t6 ] *)

(* let _ = listFromStat p  *)
