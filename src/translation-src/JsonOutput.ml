open Sparql ;;
open Yojson ;;
open Algebra ;;

let print a =
  let list_instr = ref [] in
  let add x = list_instr := x::!list_instr in
  let cur_id = ref (-1) in
  let get_id () = incr cur_id ; !cur_id in
  let rec foo cols x =
    let id = get_id() in
    let jid =  "id",`Int id in
    let assoc v l =
      try
        List.assoc v l
      with
        Not_found -> v
    in

    let op c args = add (`Assoc  ["op",`String c; jid; "arg",`Assoc args]) ; id in

    
    match x with
     | Readfile3 ->
        op "ALL" ["cols",`List (List.map (fun x-> `String (assoc x cols)) ["s";"p";"o"]) ]
     | Readfile2(f) ->
        op "PRED"  ["filename",`String ("p"^numero f);"col_subject",`String (assoc  "s" cols); "col_object",`String (assoc  "o" cols) ]
     | Filter(c,v,a) ->
        op "FILTER"  ["col",`String (assoc c cols); "value",`String (String.sub v 1 (String.length v-2)) ; "id",`Int (foo cols a) ]
     | Keep(l,a) ->
        op "SELECT"  ["cols",`List (List.map (fun c -> `String (assoc c cols)) l) ; "id",`Int (foo cols a) ]
     | Join(a,b) ->
        op "JOIN" ["id1",`Int (foo cols a) ;  "id2",`Int (foo cols b)]
     | LeftJoin(a,b) ->
        op "LEFTJOIN" ["id_base",`Int (foo cols a) ;  "id_extend",`Int (foo cols b)]
     | JoinWithBroadcast(b,a) ->
        let varname = `String ("join_"^(string_of_int (get_id ()))) in
        let _ = op "BROADCAST" ["vardef",`Int (foo cols a) ;  "varname",varname] in
        op "MAP_BC" ["id",`Int (foo cols b) ;  "var",varname]
     | Broadcast(i,a,b) ->
        let varname = `String ( "i_"^string_of_int i) in
        let def = foo cols a in
        let _ = op "BROADCAST" ["vardef",`Int def ;  "varname",varname] in
        foo cols b

     | FilterWithBroadcast(a,i,_) ->
        let varname = `String ("i_"^string_of_int i) in
        op "MAP_BC" ["id",`Int (foo cols a) ;  "var",varname]

     | Empty ->
        op "EMPTY" []

     | Order (l,a) ->
        op "ORDER" ["id",`Int (foo cols a) ; "BY",`Assoc (List.map (fun (x,y) -> (assoc x cols),`Bool y) l) ]
      
        
     | Rename(a,b,c) ->
        foo ((a,b)::cols) c       

     | Union(a,b) ->
        op "UNION"  ["id1",`Int (foo cols a) ;  "id2",`Int (foo cols b)]

     | Distinct(a) ->
        op "DISTINCT"  ["id",`Int (foo cols a)]

       
     | _ -> failwith ("error @"^__LOC__)
  in
  let final = foo [] a in
  print_string (Yojson.Basic.to_string (`Assoc ["commands",`List (List.rev !list_instr); "result_id",`Int final]))
