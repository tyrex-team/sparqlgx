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
  
selectclause:
| SELECT l = separated_list(COMMA, VAR) WHERE c = toplevel
    { (l,c) }
| SELECT JOKER WHERE c = toplevel
    { (["*"],c) }
| SELECT LEFTPAR l = separated_list(COMMA, VAR) RIGHTPAR WHERE c = toplevel
    { (l,c) }
;
  
ident_or_var:
| s = IDENT
   { Exact(s) }
| LEFTPROG pref = IDENT COLON v = IDENT RIGHTPROG
   { Exact("<"^(replace_prefix pref)^":"^(v)^">") }
| s = VAR
   { Variable(s) }
| LEFTPROG s = VAR RIGHTPROG
   { Variable(s) }
;  

toplevel:
| c=union
  {c}
| a=opt_tp
  { [a] }
;

union:
| a=opt UNION b=toplevel
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

