open Struct ;;
open ToScalaUtils ;;

(********************
Operations on Queries
********************)

let corefilter((t:triple) , (position1:int) , (position2:int)) :string =
  (*We have to know that 's' and 'd' are integers whereas 'p' is a string.*)
  match position1,position2 with
  | k,0 -> begin match k with
    | 1 -> "case (s,p,d)=>p.equals(\""^(atom2string t.pred)^"\") && d.equals(\""^(atom2string t.obj)^"\")"
    | 2 -> "case (s,p,d)=>s.equals(\""^(atom2string t.subj)^"\") && d.equals(\""^(atom2string t.obj)^"\")"
    | 3 -> "case (s,p,d)=>s.equals(\""^(atom2string t.subj)^"\") && p.equals(\""^(atom2string t.pred)^"\")"
    | _ -> failwith("Impossible case of matching in 'corefilter'.\n")
  end
  | 1,2 -> "case (s,p,d)=>d.equals(\""^(atom2string t.obj)^"\")"
  | 1,3 -> "case (s,p,d)=>p.equals(\""^(atom2string t.pred)^"\")"
  | 2,3 -> "case (s,p,d)=>s.equals(\""^(atom2string t.subj)^"\")"
  | _,_ -> failwith("Impossible case of matching in 'corefilter'.\n")
;;

let coremap((t:triple) , (position1:int) , (position2:int)) :string =
  (*We have to know that 's' and 'd' are integers whereas 'p' is a string.*)
  match position1,position2 with
  | k,0 -> begin match k with
    | 1 -> "case (s,p,d)=>s"
    | 2 -> "case (s,p,d)=>p"
    | 3 -> "case (s,p,d)=>d"
    | _ -> failwith("Impossible case of matching in 'corefilter'.\n")
  end
  | 1,2 -> "case (s,p,d)=>(s,p)"
  | 1,3 -> "case (s,p,d)=>(s,d)"
  | 2,3 -> "case (s,p,d)=>(p,d)"
  | _,_ -> failwith("Impossible case of matching in 'corefilter'.\n")
;;

let translate1 ( (triple:triple) , (e:environment) ) =
  let x,position=extractFirstVar(triple) in
  match isInEnv(x,e) with
  | None -> let newx:element = {var=x;label=e.current;nbJoin=0;varlist=[x]} in
	    let filter:string = corefilter(triple,position,0) in
	    let mapped:string = coremap(triple,position,0) in
	    let head:string = "val Q"^(string_of_int e.current)^"=triples.filter{"^filter^"}\n\t.map{"^mapped^"}" in
	    head^";\n",upEnv newx e
  | Some(t) -> let newvarlist:string list = union([x],t.varlist) in
	       let newx:element = {var=x;label=e.current;nbJoin=t.nbJoin+1;varlist=newvarlist} in
	       let filter:string = corefilter(triple,position,0) in
	       let mapped:string = coremap(triple,position,0) in
	       let key1:string = printlist [x] in
	       let key2:string = printlist t.varlist in
	       let adjustkey2:string = printlist (adjust t.varlist [x]) in
	       let union:string = printlist(newvarlist) in
	       let head:string = "val Q"^(string_of_int e.current)^"=triples.filter{"^filter^"}\n\t.map{"^mapped^"}" in
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
		 let head:string = "val Q"^(string_of_int e.current)^"=triples.filter{"^filter^"}\n\t.map{"^mapped^"}" in
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
		    let head:string = "val Q"^(string_of_int e.current)^"=triples.filter{"^filter^"}\n\t.map{"^mapped^"}" in
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
		    let head:string = "val Q"^(string_of_int e.current)^"=triples.filter{"^filter^"}\n\t.map{"^mapped^"}" in
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
      let head:string = "val Q"^(string_of_int e.current)^"=triples.filter{"^filter^"}\n\t.map{"^mapped^"}" in
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
      let head:string = "val Q"^(string_of_int e.current)^"=triples.filter{"^filter^"}\n\t.map{"^mapped^"}" in
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
