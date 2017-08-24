open Sparql ;;
open Yojson ;;
open Algebra ;;

let print a =
  let list_instr = ref [] in
  let add x = list_instr := x::!list_instr in
  let cur_id = ref (-1) in
  let get_id () = incr cur_id ; !cur_id in
  let rec foo id_of_bc cols x =
    let id = get_id() in
    let jid =  "id",`Int id in
    let assoc v l =
      try
        List.assoc v l
      with
        Not_found -> v
    in

    let op c args = add (`Assoc  ["op",`String c; jid; "arg",`Assoc args]) ; id in

    let var_at = function
      | Atom a -> "value",`String a
      | Var a -> "col",`String (assoc a cols)
    in

    match x with
     | Readfile3 ->
        op "ALL" ["cols",`List (List.map (fun x-> `String (assoc x cols)) ["s";"p";"o"]) ]
     | Readfile2(f) ->
        op "PRED"  ["filename",`String ("p"^numero f);"col_subject",`String (assoc  "s" cols); "col_object",`String (assoc  "o" cols) ]
     | Filter(Equal(c,v),a) ->
        op "FILTER"  [var_at c; var_at v ; "id",`Int (foo id_of_bc cols a) ]
     | Keep(l,a) ->
        op "SELECT"  ["cols",`List (List.map (fun c -> `String (assoc c cols)) l) ; "id",`Int (foo id_of_bc cols a) ]
     | Join(a,b) ->
        op "JOIN" ["id1",`Int (foo id_of_bc cols a) ;  "id2",`Int (foo id_of_bc cols b)]
     | LeftJoin(a,b) ->
        op "LEFTJOIN" ["id_base",`Int (foo id_of_bc cols a) ;  "id_extend",`Int (foo id_of_bc cols b)]
     | JoinWithBroadcast(b,a) ->
        let id_bc = get_id () in
        let _ = add (`Assoc ["op",`String "BROADCAST"; "id",`Int id_bc;"source",`Int (foo id_of_bc cols a)]) in
        op "MAP_BC" ["id1",`Int (foo id_of_bc cols b) ;  "id2", `Int id_bc]
     | Broadcast(i,a,b) ->
        let id_bc = get_id () in
        let def = foo id_of_bc cols a in
        let _ = add (`Assoc ["op",`String "BROADCAST"; "id",`Int id_bc; "source",`Int def ]) in
        foo ((i,id_bc)::id_of_bc) cols b

     | FilterWithBroadcast(a,i,_) ->
        op "MAP_BC" ["id1",`Int (foo id_of_bc cols a) ;  "id2",`Int (List.assoc i id_of_bc )]

     | Empty ->
        op "EMPTY" []

     | Order (l,a) ->
        op "ORDER" ["id",`Int (foo id_of_bc cols a) ; "BY",`Assoc (List.map (fun (x,y) -> (assoc x cols),`Bool y) l) ]
      
        
     | Rename(a,b,c) ->
        foo id_of_bc ((a,b)::cols) c       

     | Union(a,b) ->
        op "UNION"  ["id1",`Int (foo id_of_bc cols a) ;  "id2",`Int (foo id_of_bc cols b)]

     | Distinct(a) ->
        op "DISTINCT"  ["id",`Int (foo id_of_bc cols a)]
       
     | _ -> failwith ("error @"^__LOC__)
  in
  let final = foo [] [] a in
  print_string (Yojson.Basic.to_string (`Assoc ["commands",`List (List.rev !list_instr); "result_id",`Int final]))
