#!/bin/bash

ocamlopt -c struct.ml
ocamlopt -c parser.ml
ocamlopt -c cost.ml

ocamlopt -c toScalaVertical.ml
ocamlopt -c translatorVertical.ml
ocamlopt -o vertical_translator str.cmxa struct.cmx parser.cmx cost.cmx toScalaVertical.cmx translatorVertical.cmx

ocamlopt -c toScalaOneFile.ml
ocamlopt -c translatorOneFile.ml
ocamlopt -o one_file_translator str.cmxa struct.cmx parser.cmx cost.cmx toScalaOneFile.cmx translatorOneFile.cmx

exit 0
