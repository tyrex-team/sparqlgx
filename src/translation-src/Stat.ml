open Scanf 
open Sparql 

type 'a summary = ('a,int) Hashtbl.t * int * int * int 
    
type 'a stat =  ('a  * ('a summary)) list

type 'a combstat =  int*'a stat

let assert_equal a b = (if a<>b then (print_int a ; print_string " " ; print_int b ;failwith ("Assert failed"^__LOC__)) ; a)

let debug x = () 
                     
let combine (tot1,stat1: 'a combstat) (tot2,stat2: 'a combstat) =

  
  let cols1 = List.map fst stat1 in
  let cols2 = List.map fst stat2 in
  
  let common_cols = List.filter (fun x -> List.mem x cols2) cols1 in


  let count (tbl,nbDef,nbPerDef,totalDef) (v:'a) =
    try
      Hashtbl.find tbl v
    with Not_found -> nbPerDef
  in

  (* mult (compute_mul s1 s2) n Compute how much n "entries" in a
     table describe by s1 can appear in s1 |><| s2 

    compute_mul s1 s2 return a list of (a,b) indicating that there are
     b times where an element can be multiplied a times *)
  let compute_mul (t1,s1) (t2,s2) =

    
    let compute_mul_col s1 s2 =
      let (tbl1,nbDef1,nbPerDef1,totalDef1) = s1 in
      let (tbl2,nbDef2,nbPerDef2,totalDef2) = s2 in
      let t1_special = t1 in (* we should minus common with t2*)
      let (mul: (int*int) list) = Hashtbl.fold (fun v n ac -> (n,count s1 v)::ac) tbl2 [(nbPerDef2,t1_special)] in
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

    let size2:int =
      let get_size (tbl,nbDef,nbPerDef,totalDef) =
        Hashtbl.fold (fun v n ac->n+ac) tbl totalDef
      in
      
      let rec foo = function
        | [] -> 0
        | [a,b] -> get_size b
        | (a,b)::q -> min (get_size b) (foo q)
      in
      foo s2
    in
    match common_cols with
    | common_cols ->
       List.fold_left (fun ac c -> combine_mul ac (compute_mul_col (List.assoc c s1) (List.assoc c s2)))  [t2,t1]  common_cols 
  in

  let mul_12 = compute_mul (tot1,stat1) (tot2,stat2) in
  let mul_21 = compute_mul (tot2,stat2) (tot1,stat1) in

  debug (fun () ->
      print_string "mul_12 " ;
      List.iter (fun (a,b) -> print_string "(" ; print_int a ; print_string "," ; print_int b ; print_string ") " ) mul_12;
      print_newline();
      print_string "mul_21 " ;
      List.iter (fun (a,b) -> print_string "(" ; print_int a ; print_string "," ; print_int b ; print_string ") " ) mul_21;
      print_newline();
    ) ;
   
  
  let mult mul n =
    let rec foo = function 
      | _,[] -> 0
      | n,((a,b)::q) ->
         if b<n
         then b*a+foo (n-b,q)
         else a*n
    in
    foo (n,mul)
  in

  let new_max = min (mult mul_12 tot1) (mult mul_21 tot2) in
  let resadd t a v = Hashtbl.add t a (min v new_max) in
  
  let rec combine_common_col s1 s2  =
    let (tbl1,nbDef1,nbPerDef1,totalDef1) = s1 in
    let (tbl2,nbDef2,nbPerDef2,totalDef2) = s2 in
    let res = Hashtbl.create 17 in

    
    Hashtbl.iter (fun v n1 ->
        let n_res = 
          try
            let n2 = Hashtbl.find tbl2 v in
            min (min (mult mul_12 n1) (mult mul_21 n2)) (n1*n2)
          with
            Not_found -> min (n1*nbPerDef2) (mult mul_12 n1)
        in
        if n_res > 0 then resadd res v n_res) tbl1 ;
    Hashtbl.iter (fun v n2 ->
	if not (Hashtbl.mem res v) then
          let n_res =
            min (mult mul_21 n2) (n2*nbPerDef1)
          in
          if n_res > 0 then resadd res v n_res) tbl2 ;
    let nbDefRes = min (mult mul_12 nbDef1) (mult mul_21 nbDef2) in
    let totalDefRes = min (mult mul_12 totalDef1) (mult mul_21 totalDef2) in

    (res,nbDefRes,nbPerDef1*nbPerDef2,totalDefRes)
  in

  let common_stat = List.map (fun c -> c,combine_common_col (List.assoc c stat1) (List.assoc c stat2)) common_cols in
  

  let combine_specific (s1:'a stat) (s2:'a stat) (c1:'a list) (c2:'a list) mul =
    match (List.filter (fun x -> not (List.mem x c2)) c1) with
    | [] -> []
    | cols1_specific ->
       List.map (fun c ->
           let tbl,nbDef,nbPerDef,totalDef = List.assoc c s1 in
           let res = Hashtbl.create 17 in
           Hashtbl.iter (fun k v -> resadd res k (mult mul v)) tbl ;
           c,(res,mult mul nbDef,mult mul nbPerDef,mult mul totalDef)
         ) cols1_specific
  in
  debug (fun () ->
      print_int tot1 ; print_string " " ; print_int tot2 ; print_string "\n";
      print_int (mult mul_12 tot1) ; print_string " " ; print_int (mult mul_21 tot2) ; print_string "\n";
    ) ;

  new_max,
  ((combine_specific stat1 stat2 cols1 cols2 mul_12)@(combine_specific stat2 stat1 cols2 cols1 mul_21)@common_stat)

let fullstat filename =
  let res = Hashtbl.create 53 in
  try
       let chan = Scanning.from_file filename in
       
       let foo nb nbDef tot =
         let hshtbl = Hashtbl.create 17 in
         let nbPerDef = ref 0 in
         let totDef = ref tot in
         for i = 1 to nb do
           Scanf.bscanf chan "%d %[^\n]\n" (fun j i -> if i = "*" then nbPerDef:=j else (Hashtbl.add hshtbl i j ; totDef := !totDef - j)) ;
         done ;
         tot,(hshtbl,nbDef,!nbPerDef,!totDef)
       in
       let rec bar () =
         try
           Scanf.bscanf chan "%s %d %d %d %d\n" (fun pred col nb nbDef total -> Hashtbl.add res (pred,col) (foo nb nbDef total)) ;
           bar ()
         with | End_of_file -> (Scanning.close_in chan)
       in
       bar () ; res
  with | Sys_error s -> failwith ("Stat file problem, "^s)
  

let get_tp_stat stat tp =
  try
    match tp with
    | (_,Variable(_),_) -> failwith ("Unsupported variable predicate @"^__LOC__)
    | (Exact(s),Exact(p),Exact(o)) -> 1,[]
    | (Variable(s),Exact(p),Variable(o)) ->
       let t0,p0 = Hashtbl.find stat (p,0) in
       let t1,p1 = Hashtbl.find stat (p,1) in
       (assert_equal t0 t1),[(s,p0) ; (o,p1) ]
    | (Exact(s),Exact(p),Variable(o)) ->
       let _,(t,_,nbPerDef,_) = Hashtbl.find stat (p,0 ) in
       let nb =
         try
           Hashtbl.find t s
         with Not_found -> nbPerDef
       in
       nb,[o,(Hashtbl.create 3,nb,1,nb)]
    | (Variable(s),Exact(p),Exact(o)) ->
       let _,(t,_,nbPerDef,_) = Hashtbl.find stat (p,1) in
       let nb =
         try
           Hashtbl.find t o
         with Not_found -> nbPerDef
       in
       nb,[s,(Hashtbl.create 3,nb,1,nb)]
  with Not_found -> 0,[]
                      
(* let statFromList =  *)
(*   List.map (fun (c,(col,a,b,d)) -> *)
(*       let t = Hashtbl.create 17 in *)
(*       List.iter (fun (k,v) -> Hashtbl.add t k v) col ; *)
(*       c,(t,a,b,d)) *)
  
let listFromStat (t,l)=
  t,List.map (fun (c,(tbl,a,b,d))->  c,(Hashtbl.fold (fun a b c -> (a,b)::c) tbl [],a,b,d)) l

(* let s2 = fullstat "stat50" *)

(* let t1 = get_tp_stat s2 (Variable("?X"),Exact "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",Exact ("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent>")) *)
(* let t2 = get_tp_stat s2 (Variable("?Y"),Exact "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",Exact ("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University>")) *)
(* let t3 = get_tp_stat s2 (Variable("?Z"),Exact "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",Exact ("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department>")) *)
(* let t4 = get_tp_stat s2 (Variable("?X"),Exact "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf>",Variable("?Z")) *)
(* let t5 = get_tp_stat s2 (Variable("?Z"),Exact "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf>",Variable("?Y")) *)
(* let t6 = get_tp_stat s2 (Variable("?X"),Exact "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom>",Variable("?Y")) *)

(* let _ = listFromStat t2 *)

(* let t14 = (combine t1 t4)  *)
(* let t14_3 = combine t14 t3 *)
       
(* let t52 = combine t2 t5 *)
(* let t52__14_3 = combine t52 t14_3  *)
(* let t_all = combine t52__14_3 t6 *)
(* let _ = listFromStat t1 *)
(* let _ = listFromStat t4 *)
(* let _ = listFromStat t3 *)
(* let _ = listFromStat t14 *)
(* let _ = listFromStat t2 *)
(* let _ = listFromStat t5 *)
(* let _ = listFromStat t6 *)
(* let _ = listFromStat t52__14_3 *)
(* let _ = listFromStat t_all *)

(* let t52__14_3_y = *)
(*   match t52__14_3 with *)
(*     _,l -> match List.assoc "?Y" l with t,_,_,_ -> t *)

(* let t6_y = *)
(*   match t6 with *)
(*     _,l -> match List.assoc "?Y" l with t,_,_,_ -> t *)

(* let tall_y = *)
(*   match t6 with *)
(*     _,l -> match List.assoc "?Y" l with t,_,_,_ -> t *)

(* let _ = *)
(*   Hashtbl.iter (fun k v -> if Hashtbl.mem t6_y k then (print_string (k^" "); print_int v ; print_string " " ; print_int (Hashtbl.find t6_y k) ; print_string "\n")) t52__14_3_y *)


(* let _ = listFromStat t_all *)
(* let y = match List.assoc "?Y" t52 with (t,_,_,_) -> Hashtbl.fold (fun v n ac -> n+ac) t 0 *)
                     (* let tel = [ *)
(*     "nom",(["alice",1;"bob",5;"charles",1],1,1,1) ; *)
(*     "tel",(["1",3],5,1,5) *)
(*   ] *)

(* let mail = [ *)
(*     "nom",(["bob",1;"alice",10;"damien",1],0,0,0) ; *)
(*     "mail",(["1@",3],7,1,7) ; *)
(*     "commune", (["oui",4;"non",3],3,1,3) *)
(*   ] *)

(* let mt = [ *)
(*     "tel",([],5,1,5) ; *)
(*     "mail", ([],5,1,5)  *)
(*   ] *)
         
(* let comb_tel_mail =  (combine (statFromList tel) (statFromList mail)) )  *)
(* let _ = listFromStat ( comb_tel_mail ) *)
(* let _ = listFromStat (combine comb_tel_mail (statFromList mt)) *)
(* let _ = listFromStat (combine (combine (statFromList tel) (statFromList mail)) (statFromList tel)) *)
