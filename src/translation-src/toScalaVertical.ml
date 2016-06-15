open Struct;;

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

(***************************
 * Operations on Environment
 * - have data about a variable isInEnv
 * - nb of join => collateral of isInEnv
 * - returns the select command selectFromEnv
 * - updates with a variable upEnv
****************************)

(* Returns the element of the variable if it exists *)
let isInEnv ( (x:string) , (env:environment) ) =
  let rec aux(s,e) = match e with
    | [] -> None
    | t :: q -> if String.compare s t.var = 0
      then Some(t)
      else aux(s,q)
  in aux(x,env.suite)
;;

(* This funciton does 2 things:
   1. Add x at the correct place (where the former x was) in the list in env
   2. Change the .label and the .varlist of the other variables that are impacted by 'x'*)
let upEnv (x:element) (env:environment) =
  let rec aux(x,e,result) = match e with
    | [] -> x :: result
    | t :: q -> if String.compare x.var t.var = 0
      then aux(x,q,x :: result) (* We add the new version of 'x'. *)
      else begin 
	if List.mem t.var x.varlist = true
	then let newt = {var=t.var;label=x.label;nbJoin=t.nbJoin;varlist=x.varlist} in
	     aux(x,q,newt :: result) (* We change the version of 't'. *)
	else aux(x,q,t :: result) (* We just keep scanning. *)
      end
  in 
  let newsuite = aux(x,env.suite,[]) in
  {current=env.current;suite=newsuite}
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

(* The idea of 'adjust' is to provide a modified 'l1' where duplicates with 'l2' are a bit changed
   e.g. l1=[a;b;c] l2=[a;x;y] adjust(l1,l2)=[a1;b;c] *)
let adjust (l1:string list) (l2:string list):string list =
  let rec aux l1 l2 lresult = match l1 with
  | [] -> List.rev lresult
  | t :: q -> if List.mem t l2 = true
    then aux q l2 ((t^"bis") :: lresult) (* Obviously, a problem can occured if a variable has been named ?xbis also... *)
    else aux q l2 (t :: lresult)
  in aux l1 l2 []
;;

let printlist(l:string list):string = 
  let rec aux(l) = match l with
  | [] -> ""
  | [t] -> t
  | t :: q -> t^","^aux(q)
  in "("^aux(l)^")"
;;

let union((l1:string list),(l2:string list)):string list=
  let rec deleteDuplicate ll = (* A stupid version. *)
    match ll with 
    | [] -> []
    | x :: [] -> x :: [] 
    | x :: y :: rest -> 
      if compare x y = 0
      then deleteDuplicate (y :: rest) 
      else x :: deleteDuplicate (y :: rest) 
  in
  let l1sort = List.sort compare l1 in
  let l2sort = List.sort compare l2 in
  deleteDuplicate(List.merge compare l1sort l2sort)
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


(*************************************************************
**************************************************************
**************************************************************)

let return_biggest_label(env:environment):int=
  let rec aux l result = match l with
    | [] -> result.label
    | t :: q -> if (List.length t.varlist) >= (List.length result.varlist)
      then aux q t
      else aux q result
  in let initial:element={var="";label=0;nbJoin=0;varlist=[]} in
     aux env.suite initial
;;

let contained l1 l2 = (* We assuming that there are no duplicate values in l1 and l2 *)
  let rec aux l1 l2 n = match l1 with
    | [] -> n
    | t :: q -> if List.mem t l2 = true
      then aux q l2 (n+1)
      else aux q l2 n
  in if List.length l1 = aux l1 l2 0
    then true
    else false
;;

(* Return the element that having a varlist containing dist and having the biggest label *)
let return_good((dist:string list) , (env:environment) ):element =
  let rec aux d e el = match e with
    | [] -> el
    | t::q -> if contained d t.varlist = true
      then begin
	if el.label <= t.label
	then aux d q t
	else aux d q el
      end else
	aux d q el
  in let initial:element={var="";label=0;nbJoin=0;varlist=[]} in
     aux dist env.suite initial
;;

(* The function to translate the distingushed variables. *)
let select ( (dist:atom list) , (env:environment) ) =
  if List.hd dist = Exact("*")
  then
    let good_label:int = return_biggest_label(env) in
    "Q"^string_of_int(good_label)
  else
    let string_dist = List.map atom2string dist in
    let good_element:element = return_good(string_dist,env) in
    "Q"^string_of_int(good_element.label)^".map{case "^(printlist good_element.varlist)^"=>"^(printlist(string_dist))^"}"
;;


(*************************************************************
**************************************************************
**************************************************************)

(* The function to translate the solution modifiers. *)
let modifiers () = ".collect;";;
