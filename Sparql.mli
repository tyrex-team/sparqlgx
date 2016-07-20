type atom = Exact of string | Variable of string ;;

type prefix = (string*string) ;;

type tp = (atom*atom*atom) ;;

type bgp = tp list ;;

type optbgp = bgp*bgp ;;

type unionoptbgp = optbgp list ;;

type query = string list * unionoptbgp ;;
  

                    
