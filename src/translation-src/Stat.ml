type 'a summary = ('a,int) Hashtbl.t * int * int 
    
type 'a stat =
    ('a  * ('a summary)) list;;
    
let rec combien (col:'a) (value:'a) (stats:'a stat) = match stats with 
  | [] -> failwith "Unknown set of cols"
  | (c,(st,nbDef,nbPerDef))::q ->
     if c = col
     then
       try
	 Hashtbl.find st value
       with
	 Not_found -> nbPerDef
     else combien col value q
;;


let combine (stat1:'a stat) (stat2:'a stat) =

  let count (tbl,nbDef,nbPerDef) (v:'a) =
    try
      Hashtbl.find tbl v
    with Not_found -> nbPerDef
  in
  
  let rec combine_common_col s1 s2  =
    let (tbl1,nbDef1,nbPerDef1) = s1 in
    let (tbl2,nbDef2,nbPerDef2) = s2 in
    let res = Hashtbl.create 17 in

    Hashtbl.iter (fun v n1 -> Hashtbl.add res v (n1*count s2 v)) tbl1 ;
    Hashtbl.iter (fun v n2 ->
	if not (Hashtbl.mem res v) then
	  Hashtbl.add res v (n2*nbPerDef1)) tbl2 ;
    let nbDefRes = max nbDef1 nbDef2 in

    (res,nbDefRes,nbPerDef1*nbPerDef2)
  in


  let compute_mul s1 s2 =
    let (tbl1,nbDef1,nbPerDef1) = s1 in
    let (tbl2,nbDef2,nbPerDef2) = s2 in
    let (mul: (int*int) list) = (nbPerDef2,nbDef1*nbPerDef1)::Hashtbl.fold (fun v n ac -> (count s2 v,n)::ac) tbl1 [] in
    let rec foo = function
      | [] -> []
      | (a,b)::(c,d)::q when a=c -> foo ((a,b+d)::q)
      | a::q -> a::foo q
    in
    List.sort (fun (a,x) (b,y) -> b-a) mul |>
    foo 
  in

  let  rec combine_mul m1 m2 = match (m1,m2) with
    | [],_ | _,[] -> []
    | (a,b)::q, (c,d)::t ->
       if b <= d
       then (min a c,b)::combine_mul q ((c,d-b)::t)
       else (min a c,d)::combine_mul ((a,b-d)::q) t       
  in

  let cols1 = List.map fst stat1 in
  let cols2 = List.map fst stat2 in
  
  let common_cols = List.filter (fun x -> List.mem x cols2) cols1 in
  
  let common_stat = List.map (fun c -> c,combine_common_col (List.assoc c stat1) (List.assoc c stat2)) common_cols in
  
  let combine_specific (s1:'a stat) (s2:'a stat) (c1:'a list) (c2:'a list) =
    match (List.filter (fun x -> not (List.mem x c2)) c1) with
    | [] -> []
    | cols1_specific ->
       let size2:int =
         let get_size (tbl,nbDef,nbPerDef) =
           Hashtbl.fold (fun v n ac->n+ac) tbl (nbDef*nbPerDef)
         in

         let rec foo = function
           | [] -> failwith "empty"
           | [a,b] -> get_size b
           | (a,b)::q -> min (get_size b) (foo q)
         in
         foo stat2
       in
       
       let mul1_2 =
         List.fold_left (fun ac c -> combine_mul ac (compute_mul (List.assoc c s1) (List.assoc c s2)))  [size2,size2]  common_cols 
       in

       let mult n =
         let rec foo = function 
           | _,[] -> 0
           | n,((a,b)::q) ->
              if b<n
              then b*a+foo (n-b,q)
              else a*n
         in
         foo (n,mul1_2)
       in
       
       List.map (fun c ->
           let tbl,nbDef,nbPerDef = List.assoc c s1 in
           let res = Hashtbl.create 17 in
           Hashtbl.iter (fun k v -> Hashtbl.add res k (mult v)) tbl ;
           c,(res,mult nbDef,mult nbPerDef)
         ) cols1_specific
  in

  (combine_specific stat1 stat2 cols1 cols2)@(combine_specific stat2 stat1 cols2 cols1)@ 
    common_stat


let statFromList =
  List.map (fun (c,(col,a,b)) ->
      let t = Hashtbl.create 17 in
      List.iter (fun (k,v) -> Hashtbl.add t k v) col ;
      c,(t,a,b))
  
let listFromStat =
  List.map (fun (c,(tbl,a,b))->
      c,(Hashtbl.fold (fun a b c -> (a,b)::c) tbl [],a,b))

let tel = [
    "nom",(["alice",5;"bob",1;"charles",1],1,1) ;
    "tel",(["1",3],5,1)
  ]

let mail = [
    "nom",(["bob",5;"alice",1;"damien",1],0,0) ;
    "mail",(["1@",3],5,1) ;
    "commune", (["oui",4;"non",3],0,0)
  ]

  
let _ = listFromStat (combine (statFromList tel) (statFromList mail)) ;;
