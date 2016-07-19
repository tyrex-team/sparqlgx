type atom = Variable of string | Exact of string;;
type triple = { subj:atom ; pred:atom ; obj:atom };;
type query = { dist:atom list ; const:triple list ; union:triple list };;

type element = { var:string ; label:int ; nbJoin:int ; varlist:string list};;
type environment = { current:int ; suite:element list };;


let atom2string (a:atom):string =
  match a with
  | Variable(x) -> x
  | Exact(x) -> x
;;

(* A simple fun to distinguish variables and constraints.*)
let isVar (a:atom) = match a with
  | Variable(x) -> 1
  | Exact(x) -> 0
;;

(* A simple fun to count the number of variables in a triple*)
let nbVar (t:triple) = isVar(t.subj)+isVar(t.pred)+isVar(t.obj);;

(* A simple fun to extract the first variable of a triple giving also its position*)
let extractFirstVar(t:triple) =
  if isVar(t.subj)=1
  then (atom2string t.subj),1
  else if isVar(t.pred)=1
  then (atom2string t.pred),2
  else (atom2string t.obj),3
;;

(* A simple fun to extract the second variable of a triple*)
let extractSecondVar(t:triple) =
  if isVar(t.subj)=0
  then (atom2string t.obj),3
  else if isVar(t.pred)=1
  then (atom2string t.pred),2
  else (atom2string t.obj),3
;;
