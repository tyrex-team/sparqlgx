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
%token <string> NUMBER
%token EOF
%token PREFIX
%token LEFTPAR RIGHTPAR LEFTPROG RIGHTPROG LEFTBRACKET RIGHTBRACKET QUOTE
%token SELECT WHERE UNION OPTIONAL FILTER REGEX
%token POINT COMMA COLON JOKER EQUAL
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
;

orderlist:
| ASC LEFTPAR v=separated_list(COMMA, VAR) RIGHTPAR
  { List.map (fun x -> (x,true)) v }
| DESC LEFTPAR v=separated_list(COMMA, VAR) RIGHTPAR
  { List.map (fun x -> (x,false)) v }
;


distinct:
| DISTINCT
   { Distinct }
;

prefix:
| PREFIX s=IDENT COLON LEFTPROG v=ident RIGHTPROG
    {add_prefix (s,v)}
;				     

vars:
| l = separated_list(COMMA, VAR)
 {l}
| l = list(VAR)
 {l}
| LEFTPAR l = separated_list(COMMA, VAR) RIGHTPAR
 {l}
| LEFTPAR l = list(VAR) RIGHTPAR
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
   { if pref = "_"
     then Variable("_"^v)
     else Exact(Prefix.prefixize (replace_prefix pref v))
   }               
| LEFTPROG s = ident RIGHTPROG
   { Exact(Prefix.prefixize ("<"^(s)^">")) }
| QUOTE s = ident QUOTE
   { Exact("\\\""^s^"\\\"") }
| QUOTE s = NUMBER QUOTE
   { Exact("\\\""^s^"\\\"") }
| s = NUMBER
   { Exact(s) }
;  

complex_toplevel:
| LEFTBRACKET a = toplevel RIGHTBRACKET
  { a }
| a=toplevel UNION b=toplevel
  { Union(a,b) }
| a=toplevel OPTIONAL b=toplevel
  { Optional(a,b) }

toplevel:
| a=ptp
 { BGP(a) }
| a = complex_toplevel b=toplevel
  { Join (a,b) }
| a = complex_toplevel 
  { a }
| LEFTBRACKET a=toplevel b=filter c=toplevel RIGHTBRACKET
  { print_string "middle!\n";Filter(Join(a,c),b)}
| LEFTBRACKET a=toplevel b=filter RIGHTBRACKET
  { print_string "endd!\n";Filter(a,b)}
| LEFTBRACKET b=filter c=toplevel RIGHTBRACKET
  { print_string "beg!\n";Filter(c,b)}
;

ptp:
| a=tp POINT b=ptp
  { a::b }
| a=tp
  { [a] }
| a=tp POINT
  { [a] }
;

tp:
| sub = ident_or_var pred = ident_or_var obj = ident_or_var 
   { (sub,pred,obj)}
;  

filter:
|  FILTER LEFTPAR f=filter_expr RIGHTPAR
   { f} 
;

filter_expr:
| a = ident_or_var EQUAL b=ident_or_var
  { Equal(a,b)} 
| a = ident_or_var LEFTBRACKET b=ident_or_var
  { Less(a,b)} 
| REGEX LEFTPAR a = ident_or_var COMMA b=ident_or_var RIGHTPAR
  { Match(a,b)} 
;
