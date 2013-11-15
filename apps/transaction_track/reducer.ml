let total : int = ref 0


(* use comp in -> List.sort comp values 
 * comparator for the (order #, value) pairs
 *)
let comp (key,amt) (key',amt') = 
  if key > key' then 1
  else if key < key' then -1
  else 0 in

let (key, values) = Program.get_input() in
let sort_values = List.sort comp values in

let rec  get_total vals =
  match vals with
  | [] -> 0
  | (key, amt)::tl ->
    if amt = 0 then 
      !total= 0; get_total tl
    else 
      !total= !total + amt; get_total tl
in

get_total values;
Program.set_output((key,string_of_int(total)))
