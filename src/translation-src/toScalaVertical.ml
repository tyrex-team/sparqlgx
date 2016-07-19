open Struct ;;
open ToScalaUtils ;;
open Parser;;    (* SPARQL parser. *)
open Cost;;      (* Optimisation on triples patterns. *)

(***************************
 * Set-up the preliminaries
****************************)

let hdfspath = "DATAHDFSPATH";;

let numero(s:string):string=
  let explode (s:string):char list =
    let rec exp (i:int) (l:char list):char list = if i < 0 then l else exp (i - 1) (s.[i] :: l) in
    exp (String.length s - 1) []
  in
  let rec sum (l:char list) (result:int):int = match l with
    | [] -> result
    | t :: q -> sum q (Char.code(t) + result)
  in
  let listchar = explode s in
  let number = sum listchar 0 in
  string_of_int number
;;

let loader(t:triple):string = match t with
  (* Pour faire simple, si:
   *  1. si le predicat est Exact alors on pioche dans p.pred
   *  2. sinon on va dans init.triples *)
  | {subj=_;pred=Exact(p);obj=_} -> let pid:string = numero(p) in
				    "val p"^pid^"=sc.textFile(\""^hdfspath^pid^".pred\").map{line => val field:Array[String]=line.split(\" \"); (field(0),field(1))};"
  | _ -> "val p=sc.textFile(\""^hdfspath^"init.triples\").map{line => val field:Array[String]=line.split(\" \"); (field(0),field(1),field(2))};"
;;

let path(t:triple):string = match t with
  (* Pour faire simple, si:
   *  1. si le predicat est Exact alors on pioche dans p.pred
   *  2. sinon on va dans init.triples *)
  | {subj=_;pred=Exact(p);obj=_} -> let pid:string = numero(p) in
				    "t(\""^hdfspath^pid^".pred\")"
  | _ -> "t\""^hdfspath^"init.triples\")"
;;
  
let listLoaders(tp:triple list):string =
  let rec deleteDuplicate ll = (* A stupid version. *)
    match ll with
    | [] -> []
    | x :: [] -> x :: []
    | x :: y :: rest ->
      if compare x y = 0
      then deleteDuplicate (y :: rest)
      else x :: deleteDuplicate (y :: rest)
  in
  let rec aux1((tp:triple list),(li:string list)):string list = match tp with
    | [] -> let intermediaire=(List.sort compare li) in deleteDuplicate(intermediaire)
    | t :: q -> let element = loader t in
		aux1(q,element :: li)
  in
  let rec aux2(li,re) = match li with
    | [] -> re
    | t :: q -> aux2(q,t^"\n"^re)
  in
  aux2(List.rev(aux1(tp,[])),"")
;;

let listPaths(tp:triple list):string =
  let rec deleteDuplicate ll = (* A stupid version. *)
    match ll with
    | [] -> []
    | x :: [] -> x :: []
    | x :: y :: rest ->
      if compare x y = 0
      then deleteDuplicate (y :: rest)
      else x :: deleteDuplicate (y :: rest)
  in
  let rec aux1((tp:triple list),(li:string list)):string list = match tp with
    | [] -> let intermediaire=(List.sort compare li) in deleteDuplicate(intermediaire)
    | t :: q -> let element = path t in
		aux1(q,element :: li)
  in
  let rec aux2(li,re) = match li with
    | [] -> re
    | t :: q -> if compare re "" = 0
		then aux2(q,t) 
		else aux2(q,t^" && "^re)
  in
  aux2(List.rev(aux1(tp,[])),"")
;;

(********************
Operations on Queries
********************)

let corefilter((t:triple) , (position1:int) , (position2:int)) :string =
  (*We have to know that 's' and 'd' are integers whereas 'p' is a string.*)
  match position1,position2 with
  | k,0 -> begin match k with
    | 1 -> "=p"^numero(atom2string t.pred)^".filter{case (s,o)=>o.equals(\""^atom2string t.obj^"\")}"
    | 2 -> "=p.filter{case (s,p,o)=>s.equals(\""^atom2string t.subj^"\") && o.equals(\""^atom2string t.obj^"\")}"
    | 3 -> "=p"^numero(atom2string t.pred)^".filter{case (s,o)=>s.equals(\""^atom2string t.subj^"\")}"
    | _ -> failwith("Impossible case of matching in 'corefilter'.\n")
  end
  | 1,2 -> "=p.filter{case (s,p,o)=>o.equals(\""^atom2string t.obj^"\")}"
  | 1,3 -> "=p"^numero(atom2string t.pred)
  | 2,3 -> "=p.filter{case (s,p,o)=>s.equals(\""^atom2string t.subj^"\")}"
  | _,_ -> failwith("Impossible case of matching in 'corefilter'.\n")
;;

let coremap((t:triple) , (position1:int) , (position2:int)) :string =
  (*We have to know that 's' and 'd' are integers whereas 'p' is a string.*)
  match position1,position2 with
  | k,0 -> begin match k with
    | 1 -> "\n\t.map{case (s,o)=>s}"
    | 2 -> "\n\t.map{case (s,p,o)=>p}"
    | 3 -> "\n\t.map{case (s,o)=>o}"
    | _ -> failwith("Impossible case of matching in 'corefilter'.\n")
  end
  | 1,2 -> "\n\t.map{case (s,p,o)=>(s,p)}"
  | 1,3 -> ""
  | 2,3 -> "\n\t.map{case (s,p,o)=>(p,o)}"
  | _,_ -> failwith("Impossible case of matching in 'corefilter'.\n")
;;

let translate1 ( (triple:triple) , (e:environment) ) =
  let x,position=extractFirstVar(triple) in
  match isInEnv(x,e) with
  | None -> let newx:element = {var=x;label=e.current;nbJoin=0;varlist=[x]} in
	    let filter:string = corefilter(triple,position,0) in
	    let mapped:string = coremap(triple,position,0) in
	    let head:string = "val Q"^(string_of_int e.current)^filter^mapped in
	    head^";\n",upEnv newx e
  | Some(t) -> let newvarlist:string list = union([x],t.varlist) in
	       let newx:element = {var=x;label=e.current;nbJoin=t.nbJoin+1;varlist=newvarlist} in
	       let filter:string = corefilter(triple,position,0) in
	       let mapped:string = coremap(triple,position,0) in
	       let key1:string = printlist [x] in
	       let key2:string = printlist t.varlist in
	       let adjustkey2:string = printlist (adjust t.varlist [x]) in
	       let union:string = printlist(newvarlist) in
	       let head:string = "val Q"^(string_of_int e.current)^filter^mapped in
	       let tail:string = "\n\t.keyBy{case "^key1^"=>"^x^"}.join(Q"^(string_of_int t.label)^".keyBy{case "^key2^"=>"^x^"})\n\t.map{case (k,v)=>v}.map{case("^key1^","^adjustkey2^")=>"^union^"}" in
	       head^tail^";\n",upEnv newx e
;;

let translate2 ( (triple:triple) , (e:environment) ) =
  let x,positionx=extractFirstVar(triple) in
  let y,positiony=extractSecondVar(triple) in
  match isInEnv(x,e),isInEnv(y,e) with
  | None,None -> let newx:element = {var=x;label=e.current;nbJoin=0;varlist=[x;y]} in
		 let newy:element = {var=y;label=e.current;nbJoin=0;varlist=[x;y]} in
		 let filter:string = corefilter(triple,positionx,positiony) in
		 let mapped:string = coremap(triple,positionx,positiony) in
		 let head:string = "val Q"^(string_of_int e.current)^filter^mapped in
		 head^";\n",upEnv newy (upEnv newx e)
  | Some(t),None -> let newvarlist:string list = union([x;y],t.varlist) in
		    let newx:element = {var=x;label=e.current;nbJoin=t.nbJoin+1;varlist=newvarlist} in
		    let newy:element = {var=y;label=e.current;nbJoin=0;varlist=newvarlist} in
		    let filter:string = corefilter(triple,positionx,positiony) in
		    let mapped:string = coremap(triple,positionx,positiony) in
		    let key1:string = printlist [x;y] in
		    let key2:string = printlist t.varlist in
		    let adjustkey2:string = printlist (adjust t.varlist [x;y]) in
		    let union:string = printlist(newvarlist) in
		    let head:string = "val Q"^(string_of_int e.current)^filter^mapped in
		    let tail:string = "\n\t.keyBy{case "^key1^"=>"^x^"}.join(Q"^(string_of_int t.label)^".keyBy{case "^key2^"=>"^x^"})\n\t.map{case (k,v)=>v}.map{case("^key1^","^adjustkey2^")=>"^union^"}" in
		    head^tail^";\n",upEnv newy (upEnv newx e)
  | None,Some(t) -> let newvarlist:string list = union([x;y],t.varlist) in
		    let newx:element = {var=x;label=e.current;nbJoin=0;varlist=newvarlist} in
		    let newy:element = {var=y;label=e.current;nbJoin=t.nbJoin+1;varlist=newvarlist} in
		    let filter:string = corefilter(triple,positionx,positiony) in
		    let mapped:string = coremap(triple,positionx,positiony) in
		    let key1:string = printlist [x;y] in
		    let key2:string = printlist t.varlist in
		    let adjustkey2:string = printlist (adjust t.varlist [x;y]) in
		    let union:string = printlist(newvarlist) in
		    let head:string = "val Q"^(string_of_int e.current)^filter^mapped in
		    let tail:string = "\n\t.keyBy{case "^key1^"=>"^y^"}.join(Q"^(string_of_int t.label)^".keyBy{case "^key2^"=>"^y^"})\n\t.map{case (k,v)=>v}.map{case("^key1^","^adjustkey2^")=>"^union^"}" in
		    head^tail^";\n",upEnv newy (upEnv newx e)
  | Some(t1),Some(t2) ->
    if t1.nbJoin >= t2.nbJoin (* We want to Join first the potentially most selective *)
    then begin (* We first join with t1 *)
      let intermediatevarlist:string list = union([x;y],t1.varlist) in
      let newvarlist:string list = union(intermediatevarlist,t2.varlist)  in
      let key1:string = printlist [x;y]  in
      let key2:string = printlist t1.varlist  in
      let adjustkey2:string = printlist (adjust t1.varlist [x;y]) in
      let key3:string = printlist t2.varlist  in
      let union1:string = printlist(intermediatevarlist)  in
      let adjustkey3:string = printlist (adjust t2.varlist intermediatevarlist) in
      let union2:string = printlist(newvarlist)  in
      let tail1:string = "\n\t.keyBy{case "^key1^"=>"^x^"}.join(Q"^(string_of_int t1.label)^".keyBy{case "^key2^"=>"^x^"})\n\t.map{case (k,v)=>v}.map{case("^key1^","^adjustkey2^")=>"^union1^"}"  in
      let tail2:string = "\n\t.keyBy{case "^union1^"=>"^y^"}.join(Q"^(string_of_int t2.label)^".keyBy{case "^key3^"=>"^y^"})\n\t.map{case (k,v)=>v}.map{case("^union1^","^adjustkey3^")=>"^union2^"}" in
      let newx:element = {var=x;label=e.current;nbJoin=t1.nbJoin+1;varlist=newvarlist} in
      let newy:element = {var=y;label=e.current;nbJoin=t2.nbJoin+1;varlist=newvarlist} in
      let filter:string = corefilter(triple,positionx,positiony) in
      let mapped:string = coremap(triple,positionx,positiony) in
      let head:string = "val Q"^(string_of_int e.current)^filter^mapped in
      head^tail1^tail2^";\n",upEnv newy (upEnv newx e)
    end else begin
      let intermediatevarlist:string list = union([x;y],t2.varlist)  in
      let newvarlist:string list = union(intermediatevarlist,t1.varlist)  in
      let key1:string = printlist [x;y] in
      let key2:string = printlist t2.varlist in
      let adjustkey2:string = printlist (adjust t1.varlist [x;y]) in
      let key3:string = printlist t1.varlist in
      let union1:string = printlist(intermediatevarlist) in
      let adjustkey3:string = printlist (adjust t2.varlist intermediatevarlist) in
      let union2:string = printlist(newvarlist) in
      let tail1:string = "\n\t.keyBy{case "^key1^"=>"^y^"}.join(Q"^(string_of_int t2.label)^".keyBy{case "^key2^"=>"^y^"})\n\t.map{case (k,v)=>v}.map{case("^key1^","^adjustkey2^")=>"^union1^"}" in
      let tail2:string = "\n\t.keyBy{case "^union1^"=>"^x^"}.join(Q"^(string_of_int t1.label)^".keyBy{case "^key3^"=>"^x^"})\n\t.map{case (k,v)=>v}.map{case("^union1^","^adjustkey3^")=>"^union2^"}" in
      let newx:element = {var=x;label=e.current;nbJoin=t1.nbJoin+1;varlist=newvarlist} in
      let newy:element = {var=y;label=e.current;nbJoin=t2.nbJoin+1;varlist=newvarlist} in
      let filter:string = corefilter(triple,positionx,positiony) in
      let mapped:string = coremap(triple,positionx,positiony) in
      let head:string = "val Q"^(string_of_int e.current)^filter^mapped in
      head^tail1^tail2^";\n",upEnv newy (upEnv newx e)
    end
;;

let translate ( (t:triple) , (e:environment) ) = match nbVar(t) with
  | 1 -> translate1(t,e)
  | 2 -> translate2(t,e)
  | _ -> failwith("Une des contraintes a trop ou pas assez de variables...")
;;

(* The function to translate the list of constraints. *)
let where ( (const:triple list) , (env:environment) ) =
  let rec aux (c,e,trad) = match c with
    | [] -> trad,e
    | [t] -> let sc,new_e = translate(t,e) in
	     let updated_e:environment = {current=new_e.current+1;suite=new_e.suite} in
	     aux([],updated_e,trad^sc)
    | t :: q -> let sc,new_e = translate(t,e) in
		let updated_e:environment = {current=new_e.current+1;suite=new_e.suite} in
		aux(q,updated_e,trad^sc)
  in aux(const,env,"")
;;


let translatorVertical (v:string) = 
  let q = parse v in
  let initEnv = {current=0;suite=[]} in
  let definition = "def t (s:String):Boolean = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration).exists(new org.apache.hadoop.fs.Path(s))\n" in

  match q.union with
  | [] ->
     let preliminaires = definition^listLoaders(q.const) in
     let contrainte,newEnv = where ( no_order(q.const) , initEnv ) in
     (*let contrainte,newEnv = where ( order_by_nb_var(q.const) , initEnv ) in*)
     let selection = select ( q.dist , newEnv ) in
     let solution_modifiers = modifiers() in
     let finalquery = "val Qfinal = if (!("^listPaths(q.const)^")) {Array();} \n\t else{"^selection^solution_modifiers^"}" in
     Printf.printf "%s \n" (preliminaires^contrainte^finalquery)
  | union -> 
     let preliminaires = definition^listLoaders(q.const) in
     let contrainte,newEnv = where ( no_order(q.const) , initEnv ) in
     (*let contrainte,newEnv = where ( order_by_nb_var(q.const) , initEnv ) in*)
     let selection = select ( q.dist , newEnv ) in
     let forUnionEnv = {current=newEnv.current;suite=[]} in
     let contrainteunion,unionEnv = where ( no_order(union) , forUnionEnv ) in
     let selectionunion = select ( q.dist , unionEnv ) in
     let solution_modifiers = modifiers() in
     let finalquery = "val Qfinal = if (!("^listPaths(q.const)^")) {Array();} \n\t else{"^selection^".union("^selectionunion^")"^solution_modifiers^"}" in
     Printf.printf "%s \n" (preliminaires^contrainte^contrainteunion^finalquery)

;;
