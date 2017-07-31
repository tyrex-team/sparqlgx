
type algebra =
  | Readfile3 
  | Readfile2 of string
  | Filter of string * string * algebra
  | Keep of string list * algebra
  | Join of algebra * algebra
  | Union of algebra * algebra
  | LeftJoin of algebra * algebra
  | JoinWithBroadcast of algebra * algebra
  | Broadcast of int*algebra*algebra
  | FilterWithBroadcast of algebra * int * string list
  | Rename of string * string * algebra
  | Distinct of algebra
  | Order of (string*bool) list*algebra
  | Empty
  
(* Numero hashes predicate names; in Vertical partionning the
   (subject,object) associated with pred are stored in (numero
  pred)^".pred "*)
let numero(s:string):string=
  String.map (fun c -> if (c <'a' || c>'z') && (c<'0' || c>'9') then '_' else c) (String.lowercase s)
