type algebra =
    Readfile3 of string
  | Readfile2 of string
  | Filter of string * string * algebra
  | Keep of string list * algebra
  | Join of algebra * algebra
  | Union of algebra * algebra
  | LeftJoin of algebra * algebra
  | Rename of string * string * algebra
  | Distinct of algebra
  | Order of (string*bool)  list*algebra
                                  
val print_algebra : algebra -> unit
