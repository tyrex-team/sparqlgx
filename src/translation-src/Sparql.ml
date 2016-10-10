type atom = Exact of string | Variable of string ;;

type prefix = (string*string) ;;

type tp = (atom*atom*atom) ;;

type bgp = tp list ;;

type optbgp = bgp*bgp ;;

type unionoptbgp = optbgp list ;;

type modifier =
  | Distinct
  | OrderBy of (string*bool) list
;;

type query = (string list * unionoptbgp)*modifier list ;;

let print_atom = function
  | Exact(a) -> print_string a
  | Variable(a) -> print_string(a)

let print_tp = function
 | s,p,o -> begin 
     print_atom(s) ; print_string(" ");
     print_atom(p) ; print_string(" ");
     print_atom(o) ; print_string(" .\n");
   end

let rec list_var = function
  | Exact(_)::q -> list_var q
  | Variable(s)::q ->
     let l = list_var q in
     if List.mem s l then l else s::l
  | [] -> []
