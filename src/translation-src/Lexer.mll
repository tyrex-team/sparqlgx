(* Tokenizer for Sparql *)
{
  open Parser

  exception Lexing_error of string

  let kwd_tbl = [
      "SELECT", SELECT;
      "WHERE", WHERE;
      "PREFIX", PREFIX;
      "UNION", UNION;
      "OPTIONAL",OPTIONAL;
      "ORDER",ORDER;
      "BY",BY;
      "ASC",ASC;
      "DESC",DESC;
      "DISTINCT",DISTINCT]
  let id_or_kwd s = try List.assoc (String.uppercase s) kwd_tbl with _ -> IDENT s
  let line = ref 1
  let newline () = incr line
}

let alphanum = ['a'-'z' 'A'-'Z' '0'-'9' '_' '/' '-' '#' '~']['a'-'z' 'A'-'Z' '0'-'9' '_' '/' '-' '#' '~' '.' ]*
let var = ['?' '$']['a'-'z' 'A'-'Z' '0'-'9' '_']*
let space = ' ' | '\t'
		    
rule next_token = parse
  | '\n'
      { newline () ; next_token lexbuf }
  | space+
      { next_token lexbuf }
  | var as s { VAR(s) }
  | alphanum as id { id_or_kwd id }
  | '{'     { LEFTBRACKET }
  | '}'     { RIGHTBRACKET }
  | '('     { LEFTPAR }
  | ')'     { RIGHTPAR }
  | '<'     { LEFTPROG }
  | '>'     { RIGHTPROG }
  | ':'     { COLON }
  | '.'     { POINT }
  | ','     { COMMA }
  | '*'     { JOKER }
  | eof     { EOF }
  | _ as c  { raise (Lexing_error ("illegal character: '" ^ String.make 1 c^"' line "^(string_of_int (!line)))) }

{}


