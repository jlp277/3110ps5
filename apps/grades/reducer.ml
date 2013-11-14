open Util;;

let (key, values) = Program.get_input() in
let get_median lst =
  let len = List.length lst in
  let rec getnth n lst =
    match lst with
      | [] -> failwith "bad list index"
      | h :: t -> if n = 1 then float_of_string h else getnth (n-1) t in
  match len mod 2 with
  | 0 -> ((getnth (len / 2) lst) +. (getnth (len / 2 + 1) lst)) /. 2.
  | _ -> getnth (len / 2 + 2) lst in
let median = get_median (List.sort compare values) in
Program.set_output [string_of_float median]
