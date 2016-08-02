%{
   open Sparql
   let prefixes = ref []
   let add_prefix s = prefixes := s::(!prefixes) 
   let replace_prefix s =
       try 
         List.assoc s (!prefixes)
       with
         Not_found -> s
%}

%token <string> VAR
%token <string> IDENT
%token EOF
%token PREFIX
%token LEFTPAR RIGHTPAR LEFTPROG RIGHTPROG LEFTBRACKET RIGHTBRACKET
%token SELECT WHERE UNION OPTIONAL
%token POINT COMMA COLON JOKER

%start query

%type <Sparql.query> query
%%

query:
| pre = list(prefix) s=selectclause EOF
    { s  }
;

prefix:
| PREFIX s=IDENT LEFTPROG v=IDENT RIGHTPROG
    {add_prefix (s,v)}
;				     

vars:
| l = separated_list(COMMA, VAR)
 {l} 
| LEFTPAR l = vars RIGHTPAR
 {l}
| JOKER
 {["*"]}
 
selectclause:
| SELECT l=vars WHERE c = toplevel
    { (l,c) }
;

ident:
| s = IDENT
   {s}
| s1 = ident POINT s2 = ident
   {(s1)^"."^(s2)}
| s1 = ident COLON s2 = ident
   {(s1)^":"^(s2)}
;

ident_or_var:
| s = VAR
   { Variable(s) }
| pref = IDENT COLON v = IDENT
   { Exact((replace_prefix pref)^":"^(v)) }
| LEFTPROG s = ident RIGHTPROG
   { Exact("<"^(s)^">") }
;  

toplevel:
| c=union
  {c}
| a=opt_tp
  { [a] }
;

union:
| a=opt_tp UNION b=toplevel
  { a::b } 
| LEFTBRACKET c=union RIGHTBRACKET
  { c }


opt_tp:
| c=opt
  { c }
| a=tplist
  { a,[]}

opt:
| a=tplist OPTIONAL b=tplist 
  { a,b }
| LEFTBRACKET a=opt RIGHTBRACKET
  { a }
;

tplist:
| a=list(tp)
  { a }
| LEFTBRACKET a=list(tp) RIGHTBRACKET
  {a}
;

tp:
| sub = ident_or_var pred = ident_or_var obj = ident_or_var POINT
   { sub,pred,obj}
;  

