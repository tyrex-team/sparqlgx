type tree_mul
type bi = Big_int.big_int
   
val compute_mul :
  bi * ('a * (('b, bi) Hashtbl.t * 'c * bi * 'd)) list ->
  bi *
  ('a * (('b, Big_int.big_int) Hashtbl.t * 'e * Big_int.big_int * 'f)) list ->
  tree_mul
val mult : tree_mul -> bi -> bi
