
type trie =
  | Node of (char*trie) list*string list
  
let add_trie t s v = 

  let rec add_trie t i =
    let rec foo = function
      | (c,t)::l when (c=s.[i]) -> (c,add_trie t (i+1))::l
      | (c,t)::l -> (c,t)::foo l
      | [] -> [s.[i],add_trie t (i+1)]
    in

  match t with
  | Node(pos,pref) ->
     if i = String.length s
     then Node(pos,v::pref)
     else Node(foo pos,pref)
  in
  add_trie t 0

let find_trie t s =
  let rec foo i = function
    | Node(a,[]) -> foo (i+1) (List.assoc s.[i] a)
    | Node(a,x::t) ->
       try
         foo (i+1) (List.assoc s.[i] a)
       with _ -> (x,i)
  in
  foo 0 t

(* let t = (Node([],[]))  *)
(* let t =add_trie t "abcd" "42"  *)
(* let t =add_trie (Node([],[])) "abcd" "42"  *)
(* let c = find_trie t "abcde"  *)
  
let prefixes = ref (Node([],[]))

let load filename =
  let chan = Scanf.Scanning.from_file filename in
  let rec foo f =
    try
      Scanf.bscanf chan "%s %[^\n]\n" (fun a b -> foo (add_trie f b a))
    with
    | End_of_file -> (Scanf.Scanning.close_in chan ; f )
  in
  prefixes:=foo (Node([],[]))
  

let prefixize s =
  if s.[0] = '<' && s.[String.length s-1] = '>'
  then
    let search = String.sub s 1 (String.length s-2) in
    try
      let pre,len = find_trie (!prefixes) search in
      pre^":"^String.sub search len (String.length search-len)
    with
    | _ -> search
  else
    s
