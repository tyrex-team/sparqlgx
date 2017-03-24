open Scanf ;;
open Sparql ;;

type 'a summary = ('a,int) Hashtbl.t * int * int * int 
    
type 'a stat =  ('a  * ('a summary)) list

let combine (stat1:'a stat) (stat2:'a stat) =

 
  let cols1 = List.map fst stat1 in
  let cols2 = List.map fst stat2 in
  
  let common_cols = List.filter (fun x -> List.mem x cols2) cols1 in


  let count (tbl,nbDef,nbPerDef,totalDef) (v:'a) =
    try
      Hashtbl.find tbl v
    with Not_found -> nbPerDef
  in

  (* Compute how much an element of s1 can be multiplied by compatible
     elements from s2 Return a (a,b) indicating that there are b times
     where an element can be multiplied a times *)
  let compute_mul s1 s2 =

    
    let compute_mul_col s1 s2 =
      let (tbl1,nbDef1,nbPerDef1,totalDef1) = s1 in
      let (tbl2,nbDef2,nbPerDef2,totalDef2) = s2 in
      let (mul: (int*int) list) = (nbPerDef2,totalDef2)::Hashtbl.fold (fun v n ac -> (n,count s1 v)::ac) tbl2 [] in
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
        | [] -> failwith "empty"
        | [a,b] -> get_size b
        | (a,b)::q -> min (get_size b) (foo q)
      in
      foo s2
    in
    match common_cols with
    | [c] ->
       (compute_mul_col (List.assoc c s1) (List.assoc c s2))
    | common_cols ->
       List.fold_left (fun ac c -> combine_mul ac (compute_mul_col (List.assoc c s1) (List.assoc c s2)))  [size2,size2]  common_cols 
  in

  let mul_12 = compute_mul stat1 stat2 in
  print_string "mul_12 " ; List.iter (fun (a,b) -> print_string "(" ; print_int a ; print_string "," ; print_int b ; print_string ") " ) mul_12; print_newline();
  let mul_21 = compute_mul stat2 stat1 in
print_string "mul_21 " ; List.iter (fun (a,b) -> print_string "(" ; print_int a ; print_string "," ; print_int b ; print_string ") " ) mul_21; print_newline();
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
        if n_res > 0 then Hashtbl.add res v n_res) tbl1 ;
    Hashtbl.iter (fun v n2 ->
	if not (Hashtbl.mem res v) then
          let n_res =
            min (mult mul_21 n2) (n2*nbPerDef1)
          in
          if n_res > 0 then Hashtbl.add res v n_res) tbl2 ;
    let nbDefRes = min nbDef1 nbDef2 in

    (res,nbDefRes,nbPerDef1*nbPerDef2,min totalDef1 totalDef2)
  in

  let common_stat = List.map (fun c -> c,combine_common_col (List.assoc c stat1) (List.assoc c stat2)) common_cols in


  let combine_specific (s1:'a stat) (s2:'a stat) (c1:'a list) (c2:'a list) mul =
    match (List.filter (fun x -> not (List.mem x c2)) c1) with
    | [] -> []
    | cols1_specific ->
       List.map (fun c ->
           let tbl,nbDef,nbPerDef,totalDef = List.assoc c s1 in
           let res = Hashtbl.create 17 in
           Hashtbl.iter (fun k v -> Hashtbl.add res k (mult mul v)) tbl ;
           c,(res,mult mul nbDef,mult mul nbPerDef,mult mul totalDef)
         ) cols1_specific
  in

  (combine_specific stat1 stat2 cols1 cols2 mul_21)@(combine_specific stat2 stat1 cols2 cols1 mul_12)@ 
    common_stat

let fullstat filename =
  let res = Hashtbl.create 53 in
  try
       let chan = Scanning.from_file filename in
       
       let foo nb nbDef tot =
         let hshtbl = Hashtbl.create 17 in
         let nbPerDef = ref 0 in
         let totDef = ref tot in
         for i = 1 to nb do
           Scanf.bscanf chan "%s %d\n" (fun i j -> if i = "*" then nbPerDef:=j else (Hashtbl.add hshtbl i j ; totDef := !totDef - j)) ;
         done ;
         hshtbl,nbDef,!nbPerDef,!totDef
       in
       let rec bar () =
         try
           Scanf.bscanf chan "%s %d %d %d %d\n" (fun pred col nb nbDef total -> Hashtbl.add res (pred,col) (foo nb nbDef total)) ;
           bar ()
         with | End_of_file -> (Scanning.close_in chan)
       in
       bar () ; res
  with | Sys_error s -> failwith ("Stat file problem, "^s)
  

let get_tp_stat stat = function
  | (_,Variable(_),_) -> failwith ("Unsupported variable predicate @"^__LOC__)
  | (Exact(s),Exact(p),Exact(o)) -> []
  | (Variable(s),Exact(p),Variable(o)) ->
     [(s,Hashtbl.find stat (p,0)) ;
      (o,Hashtbl.find stat (p,1)) ]
  | (Exact(s),Exact(p),Variable(o)) ->
     let (t,nbDef,nbPerDef,totDef) = Hashtbl.find stat (p,0 ) in
     let nb =
       try
         Hashtbl.find t s
       with Not_found -> nbPerDef
     in
     [o,(Hashtbl.create 3,nb,1,nb)]
  | (Variable(s),Exact(p),Exact(o)) ->
     let (t,nbDef,nbPerDef,totDef) = Hashtbl.find stat (p,1) in
     let nb =
       try
         Hashtbl.find t o
       with Not_found -> nbPerDef
     in
     [s,(Hashtbl.create 3,nb,1,nb)]
    
                      
let s2 = fullstat "stat50"

let _ = Hashtbl.find s2 ("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",1)

let _ = get_tp_stat s2 (Variable("?Y"),Exact ("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") ,Exact "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University>")

let a = get_tp_stat s2 (Variable("?Y"),Exact ("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor>") ,Variable "?X")
let b = get_tp_stat s2 (Variable("?X"),Exact ("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#researchInterest>") ,Variable "?Z")

let c= combine a b 

let statFromList = 
  List.map (fun (c,(col,a,b,d)) ->
      let t = Hashtbl.create 17 in
      List.iter (fun (k,v) -> Hashtbl.add t k v) col ;
      c,(t,a,b,d))
  
let listFromStat =
  List.map (fun (c,(tbl,a,b,d))->
      c,(Hashtbl.fold (fun a b c -> (a,b)::c) tbl [],a,b,d))

let t1 = get_tp_stat s2 (Variable("?X"),Exact "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",Exact ("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent>"))
let t2 = get_tp_stat s2 (Variable("?Y"),Exact "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",Exact ("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University>"))
let t3 = get_tp_stat s2 (Variable("?Z"),Exact "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",Exact ("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department>"))
let t4 = get_tp_stat s2 (Variable("?X"),Exact "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf>",Variable("?Z"))
let _ = listFromStat t2
let t5 = get_tp_stat s2 (Variable("?Z"),Exact "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf>",Variable("?Y"))
let t6 = get_tp_stat s2 (Variable("?X"),Exact "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom>",Variable("?Y"))

let t14 = (combine t1 t4) 
let t14_3 = combine (combine t1 t4) t3
       
let t52 = combine t2 t5
let t52__14_3 = combine t52 t14_3 
let t_all = combine t52__14_3 t6
let _ = listFromStat t2
let _ = listFromStat t5
let _ = listFromStat t52
let _ = listFromStat t52__14_3
let _ = listFromStat t_all
let y = match List.assoc "?Y" t52 with (t,_,_,_) -> Hashtbl.fold (fun v n ac -> n+ac) t 0
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
