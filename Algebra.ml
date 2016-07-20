open Sparql ;;

type algebra =
  | Readfile3 of string
  | Readfile2 of string
  | Filter of string*string*algebra
  | Keep of (string list)*algebra
  | Join of algebra*algebra
  | Union of algebra*algebra
  | LeftJoin of algebra*algebra
  | Rename of string*string*algebra
;;
  
let rec print_algebra term = 
  
  let gid = 
     let id = ref 0 in
     fun () -> incr id ; string_of_int (!id) 
  in
  
  let lines = ref [] in
  
  let add l = lines := (l^"\n")::(!lines) in 

  let escape_var a =
    if a.[0] = '?' || a.[0] = '$'
    then let b = Bytes.copy a in (Bytes.set b 0 'V' ; b)
    else a
  in

  let rec join = function
    | [] -> ""
    | [a] -> escape_var a
    | a::q -> escape_var a^","^join q
  in

  let renamedup l1 l2 =
    (* Obviously, a problem can occured if a variable has been named ?xbis also... *)
    List.map (fun t -> if List.mem t l2 then t^"bis" else t) l1
  in

  
  let rec foo l = 
    let res = "v"^gid () in 
    let code,cols = match l with 
      | Readfile3(f) ->
         "val "^res^"=sc.textFile(\""^f^"\").map{line => val field:Array[String]=line.split(\" \"); (field(0),field(1),field(2))};",["s";"p";"o"]                                                                                                                                      
      | Readfile2(f) ->
         "val "^res^"=sc.textFile(\""^f^"\").map{line => val field:Array[String]=line.split(\" \"); (field(0),field(1))};",["s";"o"]
                                                                                                                             
      | Filter(c,v,a) ->
         let code,cols = foo a in
         "val "^res^"="^code^".filter{case ("^(join cols)^") => "^c^".equals(\""^v^"\")}",cols
                                                                                            
      | Keep (keepcols,a) ->
         let code,cols = foo a in
         "val "^res^"="^code^".map{case ("^(join cols)^") => ("^(join keepcols)^")}",keepcols
                                                                                      
      | Join(a,b)
      | LeftJoin(a,b) ->
         let code_a,cols_a = foo a
         and code_b,cols_b = foo b in
         let cols_join = List.filter (fun x -> List.mem x cols_b) cols_a in
         let cols_union = cols_a@(List.filter (fun x -> not (List.mem x cols_join)) cols_b) in
         let cols_all = cols_a@(renamedup cols_b cols_a) in
         let join_type = match l with
           | LeftJoin(_) -> "leftjoin"
           | _ -> "join"
         in
         if cols_join = []
         then
           "val "^res^"="^code_a^".cartesian("^code_b^")",cols_union
         else
           "val "^res^"="^code_a^".keyBy{case ("^(join cols_a)^") => ("^(join cols_join)^")}."^join_type^"("^code_b^".keyBy{case ("^(join cols_b)^")=>("^(join cols_join)^")}).values.map{case("^(join cols_all)^")=>("^(join cols_union)^")}",cols_union
                                                                                                                                                                                                                                                  
      | Rename(o,n,c) ->
         let code_c,cols_c = foo c in
         "val "^res^"="^code_c,(List.map (fun x -> if x=o then n else x) cols_c)


      | Union (a,b) ->
         let code_a,cols_a = foo a
         and code_b,cols_b = foo b in
         let cols_union = cols_a@(List.filter (fun x -> not (List.mem x cols_a)) cols_b) in
         let new_cols_a = List.map (fun x -> if List.mem x cols_a then x else "\"\"") cols_union in
         let new_cols_b = List.map (fun x -> if List.mem x cols_b then x else "\"\"") cols_union in
         "val "^res^"= ("^code_a^".map{case ("^(join cols_a)^")=>("^(join new_cols_a)^")}).union("^code_b^".map{case("^(join cols_b)^") => ("^(join new_cols_b)^")})",cols_union
    in
    add code ; res,cols
  in
  let code,cols = foo term in
  add (code^".collect") ;
  add ("// order is "^(join cols)) ;
  List.iter print_string (List.rev (!lines)) 
;;
  

let translate vertical stmt =

  let rec list_var = function
    | Exact(_)::q -> list_var q
    | Variable(s)::q -> s::list_var q
    | [] -> []
  in
  
  let translate_el base = function
    | Exact(v),name -> Filter(name,v,base)
    | Variable(v),name -> Rename(name,v,base)
  in

  let translate_tp = function
    | s,Exact(p),o when vertical -> Keep(list_var [s;o],List.fold_left translate_el (Readfile2(p)) [s,"s";o,"o"])
    | s,p,o -> Keep(list_var [s;p;o],List.fold_left translate_el (Readfile3("all")) [s,"s";p,"p";o,"o"])
  in

  let rec translate_list_tp = function
    | [] -> failwith "Empty list of TP"
    | [a] -> translate_tp a
    | a::q -> Join(translate_tp a,translate_list_tp q)
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
  translate_toplevel stmt
  
  (* let _ = *)
(*    print_algebra (Union(Join ( *)
(*                      Keep(["pers"],Rename("s","pers",Filter("o","21",Readfile2("age")))), *)
(*                      Keep(["gender";"pers"],Rename("o","gender",Rename("s","pers",Readfile2("gender")))) *)
(*                       ), *)
(*                       Keep(["a"],Rename("s","a",Readfile2("age") )) *)
(*                       )) *)
  
