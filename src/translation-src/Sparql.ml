type atom = Exact of string | Variable of string ;;

type prefix = (string*string) ;;

type tp = (atom*atom*atom) ;;

type bgp = tp list ;;

type optbgp = bgp*bgp ;;

type unionoptbgp = optbgp list ;;

type query = string list * unionoptbgp ;;

  
let rec list_var = function
  | Exact(_)::q -> list_var q
  | Variable(s)::q ->
     let l = list_var q in
     if List.mem s l then l else s::l
  | [] -> []



                    
