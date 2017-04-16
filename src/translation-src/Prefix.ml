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
  let rec foo = function
    | (a,b)::q ->
       if String.length b < String.length s && b=String.sub s 0 (String.length b)
       then a^":"^String.sub s (String.length b) (String.length s-String.length b-1)
       else foo q
    | [] -> s
  in
  if s.[0] = '<' && s.[String.length s-1] = '>'
  then foo (!prefixes)
  else s
