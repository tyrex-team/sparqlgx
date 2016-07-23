open Sparql ;;

let stats = Hashtbl.create 17

let () = Hashtbl.add stats ("s","*") 1 ; Hashtbl.add stats ("p","*") 1 ; Hashtbl.add stats ("o","*") 1 
  
let reorder l =

  let rec nb_var (s,p,o) =
    List.fold_left (fun ac el -> match el with Variable _ -> 1 +ac | _ -> ac) 0 [s;p;o]
  in

  let sel name value =
    try 
      Hashtbl.find stats (name,value)
    with
      Not_found ->
      Hashtbl.find stats (name,"*")
  in
  
  let grade (s,p,o) =
    let rec foo = function
    | [] -> None
    | (Variable _,n)::q -> foo q
    | (Exact v,n)::q ->
       let s = sel n v in
       Some (match foo q with
             | None -> s
             |  Some v -> min v s
            )
    in

    match foo [s,"s";p,"p";o,"o"] with
    | Some g -> (nb_var (s,p,o),g),(s,p,o)
    | None -> (0,0),(s,p,o)
  in

  let rec sort =
    List.sort (fun x y -> if x>y then 1 else -1)
  in
  
  let grade,tps = List.split (sort (List.map grade l)) in
  tps
;;
