#!/bin/bash

rm -f vertical_translator one_file_translator *.cm[xio] *.o

ocamlopt -c struct.ml
ocamlopt -c parser.ml
ocamlopt -c cost.ml

ocamlopt -c toScalaUtils.ml
ocamlopt -c toScalaVertical.ml
#ocamlopt -c translatorVertical.ml
#ocamlopt -o vertical_translator str.cmxa struct.cmx parser.cmx cost.cmx toScalaUtils.cmx 

ocamlopt -c toScalaUtils.ml
ocamlopt -c toScalaOneFile.ml
#ocamlopt -c translatorOneFile.ml
ocamlopt -c main.ml
ocamlopt -o one_file_translator str.cmxa struct.cmx parser.cmx cost.cmx toScalaUtils.cmx toScalaOneFile.cmx toScalaVertical.cmx 
cp one_file_translator vertical_translator
exit 0
