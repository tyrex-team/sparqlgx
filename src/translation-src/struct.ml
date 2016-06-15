type 'a part = None | Some of 'a;;

type atom = Variable of string | Exact of string;;
type triple = { subj:atom ; pred:atom ; obj:atom };;
type query = { dist:atom list ; const:triple list ; union:triple list };;

type element = { var:string ; label:int ; nbJoin:int ; varlist:string list};;
type environment = { current:int ; suite:element list };;
