%{
   open Sparql
   let prefixes = ref []
   let add_prefix (k,s) = prefixes := (k,s) ::(!prefixes) 
   let replace_prefix s v =
       try 
         "<"^List.assoc s (!prefixes)^v^">"
       with
         Not_found -> s^":"^v
%}

%token <string> VAR
%token <string> IDENT
%token EOF
%token PREFIX
%token LEFTPAR RIGHTPAR LEFTPROG RIGHTPROG LEFTBRACKET RIGHTBRACKET
%token SELECT WHERE UNION OPTIONAL
%token POINT COMMA COLON JOKER
%token DISTINCT ORDER BY ASC DESC
%start query

%type <Sparql.query> query
%%


query:
| pre = list(prefix) SELECT  dis=distinct? l=vars WHERE c = toplevel ord=orderby? EOF
    { (l,c),(List.fold_left (fun ac el -> match el with | Some v ->v::ac | None -> ac ) [] [dis;ord])   }
;


orderby:
| ORDER BY v=nonempty_list(orderlist) EOF
   { OrderBy(List.flatten v) }
| ORDER BY v=separated_list(COMMA, VAR) EOF
  { OrderBy(List.map (fun x -> (x,true)) v) }

orderlist:
| ASC LEFTPAR v=separated_list(COMMA, VAR) RIGHTPAR
  { List.map (fun x -> (x,true)) v }
| DESC LEFTPAR v=separated_list(COMMA, VAR) RIGHTPAR
  { List.map (fun x -> (x,false)) v }



distinct:
| DISTINCT
   { Distinct }

prefix:
| PREFIX s=IDENT COLON LEFTPROG v=ident RIGHTPROG
    {add_prefix (s,v)}
;				     

vars:
| l = separated_list(COMMA, VAR)
 {l} 
| LEFTPAR l = separated_list(COMMA, VAR) RIGHTPAR
 {l}
| JOKER
 {["*"]}

ident:
| s=separated_list(COLON,IDENT)
   {List.fold_left (fun ac el -> match ac with  | "" ->el | ac -> ac^":"^el ) "" s}
;

ident_or_var:
| s = VAR
   { Variable(s) }
| pref = IDENT COLON v = IDENT
   { Exact(Prefix.prefixize (replace_prefix pref v)) }
| LEFTPROG s = ident RIGHTPROG
   { Exact(Prefix.prefixize ("<"^(s)^">")) }
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

ptp:
| a=tp POINT b=ptp
  { a::b }
| a=tp
  { [a] }
| a=tp POINT
  { [a] }

tplist:
| a=ptp
  { a }
| LEFTBRACKET a=ptp RIGHTBRACKET
  {a}
;

tp:
| sub = ident_or_var pred = ident_or_var obj = ident_or_var 
   { sub,pred,obj}
;  

