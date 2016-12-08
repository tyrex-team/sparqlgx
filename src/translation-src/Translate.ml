open Sparql ;;
open Reorder ;;
open Algebra ;;
let print_query distinguished modifiers optim stmt =
  (* let print_list_tp l = *)
  (*   let rec foo = function *)
  (*     | [] -> failwith "Empty list of TP" *)
  (*     | [a] -> print_tp a *)
  (*     | a::q -> print_tp a ; foo q *)
  (*   in *)
  (*   failwith "not supported"  *)
  (*   (\* match optim with *\) *)
  (*   (\* | 0 -> foo (List.rev l) *\) *)
  (*   (\* | 1 -> foo (Reorder.no_cartesian l) *\) *)
  (*   (\* | 2 -> foo (Reorder.reorder l) *\) *)
  (*   (\* | _ -> foo (Reorder.no_cartesian l) *\) *)
  (* in *)

  (* let print_opt  = function *)
  (*   | (a,[]) -> print_list_tp a *)
  (*   | (a,b) -> print_list_tp a ; print_list_tp b *)
  (* in *)

  (* let rec print_toplevel = function *)
  (*   | [] -> failwith "Empty query!" *)
  (*   | [a] -> print_opt a *)
  (*   | a::q -> print_opt a ; print_string "UNION {" ; print_toplevel q ; print_string " }" *)
  (* in *)

  (* let rec print_list_order = function *)
  (*   | [] -> print_string "" *)
  (*   | (x,true)::q -> print_string x ; print_list_order q *)
  (*   | _::q -> print_list_order q *)
  (* in *)

  (* let rec print_modifiers bgp = function *)
  (*   | [] ->  *)
  (*      begin *)
  (*       List.iter (Printf.printf "%s ") distinguished ; *)
  (*       print_string "\nWHERE {\n" ; *)
  (*       print_toplevel stmt ; *)
  (*       print_string "}\n" ; *)
  (*      end *)
  (*   | OrderBy(l)::q -> *)
  (*      begin *)
  (*       print_modifiers bgp q ; *)
  (*       print_string "Order By { " ; *)
  (*       print_list_order l ; *)
  (*       print_string " }" ; *)
  (*      end *)
  (*   | Distinct::q -> *)
  (*      begin  *)
  (*       print_string "DISTINCT " ;  *)
  (*       print_modifiers bgp q ; *)
  (*      end *)
  (* in *)
  
  (* print_string "SELECT "; *)
  (* print_modifiers stmt modifiers ; *)
()
;;

  
let translate distinguished modifiers vertical optim stmt =
  
  let translate_el (base,cols) = function
    | Exact(v),name -> (Filter(name,"\""^v^"\"",base),cols)
    | Variable(v),name ->
       if List.mem v cols
       then Filter(name,v,base),cols
       else (Rename(name,v,base),v::cols)
  in

  let translate_tp = function
    | s,Exact(p),o when vertical -> Keep(list_var [s;o],fst (List.fold_left translate_el (Readfile2(p),[]) [s,"s";o,"o"]))
    | s,p,o -> Keep(list_var [s;p;o],fst (List.fold_left translate_el (Readfile3("all"),[]) [s,"s";p,"p";o,"o"]))
  in

  let translate_list_tp l =
    let rec foo = function
      | [] -> failwith "Empty list of TP"
      | [a] -> translate_tp a
      | a::q -> Join(translate_tp a,foo q)
    in
    let rec foo x = x in
    match optim with
    | 0 -> foo (Reorder.no_optim translate_tp (List.rev l))
    | 1 -> foo (Reorder.no_cartesian translate_tp l)
    | 2 -> foo (Reorder.reorder  translate_tp l)
    | _ -> foo (Reorder.no_cartesian  translate_tp l)
  in

  let translate_opt  = function
    | (a,[]) -> translate_list_tp a
    | (a,b) -> LeftJoin(translate_list_tp a,translate_list_tp b)
  in

  let rec translate_toplevel = function
    | [] -> failwith "Empty query!"
    | [a] -> translate_opt a
    | a::q -> Union(translate_opt a,translate_toplevel q)
  in

  let rec add_modifiers t = function
    | [] -> t
    | OrderBy(l)::q -> Order(l,add_modifiers t q)
    | Distinct::q -> Distinct(add_modifiers t q)
  in
  
  add_modifiers (match distinguished with
   | ["*"] -> translate_toplevel stmt
   | _ ->  Keep(distinguished,  translate_toplevel stmt))
   modifiers 
  
  (* let _ = *)
(*    print_algebra (Union(Join ( *)
(*                      Keep(["pers"],Rename("s","pers",Filter("o","21",Readfile2("age")))), *)
(*                      Keep(["gender";"pers"],Rename("o","gender",Rename("s","pers",Readfile2("gender")))) *)
(*                       ), *)
(*                       Keep(["a"],Rename("s","a",Readfile2("age") )) *)
(*                       )) *)
  
