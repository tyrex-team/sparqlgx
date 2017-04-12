let minus l1 l2 = List.filter (fun x -> not (List.mem x l2)) l1

let inter l1 l2 = List.filter (fun x -> List.mem x l2) l1

let union l1 l2 =  l1@minus l2 l1
