open Sparql ;;
open Scanf ;;
  
let stats = Hashtbl.create 17

(* let () = Hashtbl.add stats ("s","*") 1 ; Hashtbl.add stats ("p","*") 1 ; Hashtbl.add stats ("o","*") 1 *)

let load filename =
  (try
    let chan = Scanning.from_file filename in
    
    let rec foo name nb =
      for i = 0 to nb-1 do
        Scanf.bscanf chan "%s %d\n" (fun i j -> Hashtbl.add stats (name,i) j) ;
      done
    in
    try
      Scanf.bscanf chan "%d %d %d\n" (fun i j k -> foo "s" i ; foo "p" j ; foo "o" k) ;
    with | End_of_file -> Scanning.close_in chan
  with | Sys_error s -> failwith ("Stat file problem, "^s)) ;

(* Hashtbl.iter (fun (x,y) z -> print_string ("("^x^","^y^") "^(string_of_int z)^"\n")) stats *)

;;
  

let no_cartesian  = 
    let rec no_cartesian seen_cols = function
      | [] -> []
      | x::t ->
         let rec find_first = function
           | [] -> raise Not_found
           | (s,p,o)::q ->
              if List.exists (fun x -> List.mem x seen_cols) (list_var [s;p;o])
              then (s,p,o),q
              else
                let fi,tl = find_first q in
                fi,(s,p,o)::tl
         in
         let (s,p,o),ntl = try  find_first (x::t) with Not_found -> x,t in
         (s,p,o)::(no_cartesian ((list_var [s;p;o])@seen_cols) ntl)
    in
    no_cartesian []
;;


let reorder l =  
             
  let rec nb_var (s,p,o) =
    List.fold_left (fun ac el -> match el with Variable _ -> 1 +ac | _ -> ac) 0 [s;p;o]
  in

  let sel name value =
    try 
      Hashtbl.find stats (name,value)
    with Not_found ->
      try 
        Hashtbl.find stats (name,"*")
      with Not_found -> -1
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

  no_cartesian tps
;;
