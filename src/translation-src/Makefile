MLI=
CMX=struct.cmx parser.cmx cost.cmx toScalaUtils.cmx toScalaOneFile.cmx toScalaVertical.cmx
CMO=struct.cmo parser.cmo cost.cmo toScalaUtils.cmo toScalaOneFile.cmo toScalaVertical.cmo
CMXA=str.cmxa
CMI=
#GENERATED=Parser.ml Lexer.ml
GENERATED=
FLAGS=-annot
OCAMLC=ocamlc
OCAMLOPT=ocamlopt

all: depend vertical onefile

debug: $(MLI) $(CMI) $(CMO)
	ocamlc -g $(CMXA) $(CMO) 

vertical: $(MLI) $(CMI) $(CMX)
	$(OCAMLOPT) $(FLAGS) -o $@ $(CMXA) $(CMX)

onefile: $(MLI) $(CMI) $(CMX)
	$(OCAMLOPT) $(FLAGS) -o $@ $(CMXA) $(CMX)

.SUFFIXES: .mli .ml .cmi .cmo .cmx .mll .mly

%.cmi: %.mli
	$(OCAMLOPT) $(FLAGS) -c  $<

.ml.cmx: $(CMI) $(MLI)
	$(OCAMLOPT) $(MLI) $(FLAGS) -c $<

%.cmo: %.ml $(MLI) $(CMI)
	$(OCAMLC) -g -c $<

.mll.ml:
	ocamllex $<

.mly.ml:
	menhir -v $<

.mly.mli:
	menhir -v $<


clean:
	rm -f *.cm[iox] *.o *.annot *~ vertical onefile $(GENERATED)
	rm -f Parser.output Parser.automaton .depend a.out

.depend depend:$(GENERATED)
	rm -f .depend
	ocamldep *.ml *.mli > .depend

include .depend

