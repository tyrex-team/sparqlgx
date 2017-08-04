type atom = Exact of string | Variable of string 

type prefix = (string*string) 

type filter_expr =
  | Equal of atom*atom
  | Less of atom * atom
  | Match of atom*atom                     

type tp =
  |  TP of atom*atom*atom
      

type bgp = (atom*atom*atom) list 

type gp =
  | Union of gp*gp
  | Optional of gp*gp
  | BGP of bgp
  | Join of gp*gp
  | Filter of gp*filter_expr
          
type modifier =
  | Distinct
  | OrderBy of (string*bool) list


type query = (string list * gp)*modifier list 

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

let rec bgp_var l =
  List.fold_left (fun ac (a,b,c) -> ListSet.union ac (list_var [a;b;c])) ListSet.empty l
        
exception TypeError of string

let rec prob = function
  | Union(a,b)
    | Join(a,b)
    | Optional(a,b) -> ListSet.union (prob a) (prob b)
  | Filter(a,b) -> prob a
  | BGP(a) -> bgp_var a

let rec cert = function
  | Union(a,b)
    | Optional(a,b) -> ListSet.inter (cert a) (cert b)
  | Join(a,b) -> ListSet.inter (cert a) (cert b)
  | Filter(a,b) -> cert a
  | BGP(a) -> bgp_var a
                     
let rec typecheck = function
  | BGP(a) -> ()
  | Optional(a,b)    
    | Join(a,b)->
     let dif = ListSet.minus (ListSet.inter (prob a) (prob b)) (ListSet.inter (cert a) (cert b)) in
     if dif <> [] then  raise ( TypeError (List.hd dif))
  | Union(a,b) -> ()
  | Filter(a,b) -> ()
 
