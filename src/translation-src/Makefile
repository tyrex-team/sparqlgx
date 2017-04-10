MLI=
CMX=Sparql.cmx Stat.cmx Parser.cmx Lexer.cmx Algebra.cmx Plan.cmx Reorder.cmx Translate.cmx main.cmx
#CMO=struct.cmo parser.cmo cost.cmo toScalaUtils.cmo toScalaOneFile.cmo toScalaVertical.cmo
CMXA=Sparql.cmxa Stat.cmxa Parser.cmxa Lexer.cmxa Algebra.cmxa Plan.cmxa Reorder.cmxa Translate.cmxa main.cmxa
CMO=Sparql.cmo Stat.cmo Parser.cmo Lexer.cmo Algebra.cmo Plan.cmo Reorder.cmo Translate.cmo main.cmo
#str.cmxa
CMI=
GENERATED=Parser.ml Lexer.ml Parser.mli
#GENERATED=
FLAGS= -annot nums.cmxa -cclib -lnums
FLAGS_DEBUG= -p nums.a nums.cma -cclib -lnums
OCAMLC=ocamlc
OCAMLOPT=ocamlopt

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
	rm -f *.cm[iox] *.o *.annot *~ sparql2scala $(GENERATED)
	rm -f Parser.output Parser.automaton Parser.conflicts .depend a.out

.depend depend:$(GENERATED)
	rm -f .depend
	ocamldep *.ml *.mli > .depend

include .depend

