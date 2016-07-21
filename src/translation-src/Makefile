MLI=
CMX=Parser.cmx Lexer.cmx Algebra.cmx main.cmx
#CMO=struct.cmo parser.cmo cost.cmo toScalaUtils.cmo toScalaOneFile.cmo toScalaVertical.cmo
CMXA=
#str.cmxa
CMI=
GENERATED=Parser.ml Lexer.ml Parser.mli
#GENERATED=
FLAGS=-annot
OCAMLC=ocamlc
OCAMLOPT=ocamlopt

all: depend sparql2scala

debug: $(MLI) $(CMI) $(CMO)
	ocamlc -g $(CMXA) $(CMO) 

sparql2scala: $(MLI) $(CMI) $(CMX)
	$(OCAMLOPT) $(FLAGS) -o $@ $(CMXA) $(CMX)


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
	rm -f Parser.output Parser.automaton .depend a.out

.depend depend:$(GENERATED)
	rm -f .depend
	ocamldep *.ml *.mli > .depend

include .depend

