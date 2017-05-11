MLI=
CMX=ListSet.cmx Prefix.cmx Sparql.cmx Stat_mult.cmx Stat_combine.cmx Parser.cmx Lexer.cmx Algebra.cmx Plan.cmx Reorder.cmx Translate.cmx main.cmx
#CMO=struct.cmo parser.cmo cost.cmo toScalaListSet.cmo toScalaOneFile.cmo toScalaVertical.cmo
CMXA= ListSet.cmxa  Prefix.cmxa Sparql.cmxa Stat_mult.cmxa Stat_combine.cmxa Parser.cmxa Lexer.cmxa Algebra.cmxa Plan.cmxa Reorder.cmxa Translate.cmxa main.cmxa
CMO=ListSet.cmo  Prefix.cmo Sparql.cmo Stat_mult.cmo Stat_combine.cmo Parser.cmo Lexer.cmo Algebra.cmo Plan.cmo Reorder.cmo Translate.cmo main.cmo 
#str.cmxa
CMI=
GENERATED=Parser.ml Lexer.ml Parser.mli
#GENERATED=
FLAGS= -annot nums.cmxa -cclib -lnums -package yojson  -linkpkg
FLAGS_DEBUG= -p nums.a nums.cma -cclib -lnums 
OCAMLC=ocamlfind ocamlc
OCAMLOPT=ocamlfind ocamlopt

all: depend sparql2scala

debug: $(MLI) $(CMI) $(CMO)
	ocamlc $(FLAGS_DEBUG) -g $(CMXA) $(CMO) 

sparql2scala: $(MLI) $(CMI) $(CMX)
	$(OCAMLOPT) $(FLAGS) -o $@  $(CMX)


.SUFFIXES: .mli .ml .cmi .cmo .cmx .mll .mly

%.cmi: %.mli
	$(OCAMLOPT) $(FLAGS) -c  $<

.ml.cmx: $(CMI) $(MLI)
	$(OCAMLOPT) $(MLI) $(FLAGS) -c $<

%.cmo: %.ml $(MLI) $(CMI)
	$(OCAMLC) -g -c $<

.mll.ml: $(GENERATED)
	ocamllex $<

.mly.ml: $(GENERATED)
	menhir -v $<

.mly.mli: $(GENERATED)
	menhir -v $<

clean:
	rm -f *.cm[ioxa] *.o *.annot *~ sparql2scala $(GENERATED)
	rm -f Parser.output Parser.automaton Parser.conflicts .depend a.out

.depend depend:$(GENERATED)
	rm -f .depend
	ocamldep *.ml *.mli > .depend

include .depend

