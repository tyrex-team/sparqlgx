open Sparql 

open Big_int
type bi = Big_int.big_int
   
type 'a summary = ('a,bi) Hashtbl.t * bi * bi * bi 
    
type 'a stat =  ('a  * ('a summary)) list

type 'a combstat =  bi*'a stat
let bioi = big_int_of_int 
let assert_equal a b = (if a<>b then (print_int a ; print_string " " ; print_int b ;failwith ("Assert failed"^__LOC__)) ; a)
let assert_equal_bi a b = (if not (eq_big_int a b) then (print_int (int_of_big_int a) ; print_string " " ; print_int (int_of_big_int b) ;failwith ("Assert failed"^__LOC__)) ; a)

let debug x = () 
         
let combine (tot1,stat1:'a combstat) (tot2,stat2:'a combstat) =

  let cols1 = List.map fst stat1 in
  let cols2 = List.map fst stat2 in
  
  let common_cols = ListSet.inter cols1 cols2 in


  
  (* mult (compute_mul s1 s2) n Compute how much n "entries" in a
     table describe by s1 can appear in s1 |><| s2 

    compute_mul s1 s2 return a list of (a,b) indicating that there are
     b times where an element can be multiplied a times *)



  

  let mul_12 = Stat_mult.compute_mul (tot1,stat1) (tot2,stat2) in
  let mul_21 = Stat_mult.compute_mul (tot2,stat2) (tot1,stat1) in  
  (* debug (fun () -> *)
  (*     print_string "mul_12 " ; *)
  (*     List.iter (fun (a,b) -> print_string "(" ; print_int a ; print_string "," ; print_int b ; print_string ") " ) mul_12; *)
  (*     print_newline(); *)
  (*     print_string "mul_21 " ; *)
  (*     List.iter (fun (a,b) -> print_string "(" ; print_int a ; print_string "," ; print_int b ; print_string ") " ) mul_21; *)
  (*     print_newline(); *)
  (*   ) ; *)
  
  (* let turns = ref 0 in *)
  (* let mult mul n = *)
  (*   let rec foo cur = function  *)
  (*     | _,[] -> cur *)
  (*     | n,((a,b)::q) -> *)
  (*        if lt_big_int b n *)
  (*        then (incr turns ; foo (add_big_int (mult_big_int b a) cur) (sub_big_int n b,q)) *)
  (*        else  add_big_int cur (mult_big_int a n) *)
  (*   in *)
  (*   foo zero_big_int (n,mul) *)
  (* in *)
  
  let new_max = min_big_int (Stat_mult.mult mul_12 tot1) (Stat_mult.mult mul_21 tot2) in
  let resadd t a v = Hashtbl.add t a (min_big_int v new_max) in
  
  let rec combine_common_col s1 s2  =
    let (tbl1,nbDef1,nbPerDef1,totalDef1) = s1 in
    let (tbl2,nbDef2,nbPerDef2,totalDef2) = s2 in
    let res = Hashtbl.create 17 in

    
    Hashtbl.iter (fun v n1 ->
        let n_res = 
          try
            let n2 = Hashtbl.find tbl2 v in
            min_big_int (Stat_mult.total mul_12 n1 n2) (Stat_mult.total mul_21 n2 n1)
          with
            Not_found -> Stat_mult.total  mul_12 n1 nbPerDef2
        in
        if sign_big_int n_res = 1 then resadd res v n_res) tbl1 ;
    Hashtbl.iter (fun v n2 ->
	if not (Hashtbl.mem res v) then
          let n_res = Stat_mult.total mul_21 n2 nbPerDef1 in
          if sign_big_int n_res = 1 then resadd res v n_res) tbl2 ;
    let nbDefRes = min_big_int  nbDef1 nbDef2 in
    let totalDefRes = min_big_int (Stat_mult.total mul_12 totalDef1 nbPerDef2) (Stat_mult.total mul_21 totalDef2 nbPerDef1) in

    (res,min_big_int nbDefRes new_max,min_big_int new_max (mult_big_int nbPerDef1 nbPerDef2),min_big_int new_max totalDefRes)
  in

  let common_stat = List.map (fun c -> c,combine_common_col (List.assoc c stat1) (List.assoc c stat2)) common_cols in
  

  let combine_specific (s1:'a stat) (s2:'a stat) (c1:'a list) (c2:'a list) mul =
    match (List.filter (fun x -> not (List.mem x c2)) c1) with
    | [] -> []
    | cols1_specific ->
       List.map (fun c ->
           let tbl,nbDef,nbPerDef,totalDef = List.assoc c s1 in
           let res = Hashtbl.create 17 in
           Hashtbl.iter (fun k v -> resadd res k (Stat_mult.mult mul v)) tbl ;
           c,(res,min_big_int new_max (Stat_mult.mult mul nbDef),min_big_int new_max (Stat_mult.mult mul nbPerDef),min_big_int new_max (Stat_mult.mult mul totalDef))
         ) cols1_specific
  in
  (* debug (fun () -> *)
  (*     print_int tot1 ; print_string " " ; print_int tot2 ; print_string "\n"; *)
  (*     print_int (mult mul_12 tot1) ; print_string " " ; print_int (mult mul_21 tot2) ; print_string "\n"; *)
  (*   ) ; *)
  let r = 
    new_max,
    ((combine_specific stat1 stat2 cols1 cols2 mul_12)@(combine_specific stat2 stat1 cols2 cols1 mul_21)@common_stat)
  in
  
  (* print_int (List.length (mul_12)) ; print_string " " ; print_int (List.length (mul_21) ) ; print_string " "; print_int (!turns) ; print_newline(); *)
  r

  
let fullstat filename : (string*int,bi*string summary) Hashtbl.t  =
  let res = Hashtbl.create 53 in
  try
       let chan = Scanf.Scanning.from_file filename in
       
       let foo (nb:int) (nbDef:bi) (tot:bi) =
         let hshtbl = Hashtbl.create 17 in
         let nbPerDef = ref zero_big_int in
         let totDef = ref tot in
         for i = 1 to nb do
           Scanf.bscanf chan "%d %[^\n]\n"
                        (fun j i ->
                          let j = bioi j in
                          if i = "*"
                          then nbPerDef:=j
                          else
                            begin
                              Hashtbl.add hshtbl i j ;
                              totDef := sub_big_int (!totDef) j
                            end
                        ) ;
         done ;
         tot,(hshtbl,nbDef,!nbPerDef,!totDef)
       in
       let rec bar () =
         try
           Scanf.bscanf chan "%s %d %d %d %d\n" (fun pred col nb nbDef total -> Hashtbl.add res (pred,col) (foo nb (bioi nbDef) (bioi total))) ;
           bar ()
         with | End_of_file -> (Scanf.Scanning.close_in chan)
       in
       bar () ; res
  with | Sys_error s -> failwith ("Stat file problem, "^s)
  

let empty_stat cols =
  zero_big_int, (List.map (fun c -> c,(Hashtbl.create 3,zero_big_int,zero_big_int,zero_big_int)) cols)
                      
let get_tp_stat stat tp : string combstat =
  try
    match tp with
    | (_,Variable(_),_) -> failwith ("Unsupported variable predicate @"^__LOC__)
    | (Exact(s),Exact(p),Exact(o)) -> unit_big_int,[]
    | (Variable(s),Exact(p),Variable(o)) ->
       let t0,p0 = Hashtbl.find stat (p,0) in
       let t1,p1 = Hashtbl.find stat (p,1) in
       (assert_equal_bi t0 t1),[(s,p0) ; (o,p1) ]
    | (Exact(s),Exact(p),Variable(o)) ->
       let _,(t,_,nbPerDef,_) = Hashtbl.find stat (p,0 ) in
       let nb =
         try
           Hashtbl.find t s
         with Not_found -> nbPerDef
       in
       nb,[o,(Hashtbl.create 3,nb,unit_big_int,nb)]
    | (Variable(s),Exact(p),Exact(o)) ->
       let _,(t,_,nbPerDef,_) = Hashtbl.find stat (p,1) in
       let nb =
         try
           Hashtbl.find t o
         with Not_found -> nbPerDef
       in
       nb,[s,(Hashtbl.create 3,nb,unit_big_int,nb)]
  with Not_found ->
    (* (match tp with | _,Exact p, _ ->failwith ("Unkown "^p)); *)
    zero_big_int,[]
                      
