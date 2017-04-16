let prefixes = ref []

let load filename =
  let chan = Scanf.Scanning.from_file filename in
  try 
    while true
    do
      Scanf.bscanf chan "%s %[^\n]\n" (fun a b -> prefixes := (a,b)::!prefixes)
    done 
  with | End_of_file -> (Scanf.Scanning.close_in chan)

let prefixize s =
  if s.[0] = '<' && s.[String.length s-1] = '>'
  then
    let search = String.sub s 1 (String.length s-2) in
    let rec foo = function
      | (a,b)::q ->
         if String.length b <= String.length search && b=String.sub search 0 (String.length b)
         then a^":"^String.sub search (String.length b) (String.length search-String.length b)
         else foo q
      | [] -> s
    in
    foo (!prefixes)
  else s
