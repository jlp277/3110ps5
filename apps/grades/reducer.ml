open Util;;

let (key, values) = Program.get_input() in
let get_median lst =
  let rec getnth n lst =
    match lst with
      | [] -> failwith "bad list index"
      | h :: t -> if n = 1 then h else getnth (n-1) t in
  let len = List.length lst in
  match len mod 2 with
  | 0 -> ((getnth (len/2) lst)+.(getnth (len/2+1) lst))/.2.
  | _ -> getnth (len/2+1) lst in
let f_values = List.map (fun x -> float_of_string x) values in
let median = get_median (List.sort compare f_values) in
Program.set_output [string_of_float median]
