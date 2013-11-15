open Util;;

let num_trans : int = ref -1
let trans_data : string list = ref []
let num_in : int = ref -1
let num_out : int = ref -1

let results : (string * string) list = ref []
let (key,value) = Program.get_input()
let block = List.map (String.lowercase (Util.split_spaces value))
let parse () =
  num_trans :=
    match block with
    | h::_ -> int_of_string h
    | [] -> failwith "bad block";
  trans_data :=
    match block with
    | _::t -> t
    | [] -> failwith "bad block";
  while !num_trans > 0 do
    let _ =
      match !trans_data with
      | h::h'::t ->
        num_in := int_of_string h;
        num_out := int_of_string h';
        trans_data := t;
      | [] -> failwith "bad data" in
    while !num_in > 0 do
      match !trans_data with
      | coinkey::t ->
        results := !results @ (coinkey,Util.marshal (key,0));
        trans_data := t;
        num_in := !num_in - 1;
      | [] -> failwith "expected incoins, no incoins"
    done;
    while !num_out > 0 do
      match !trans_data with
      | coinkey::amt::t ->
        results := !results @ (coinkey,Util.marshal (key,int_of_string amt));
        trans_data := t;
        num_out := !num_out - 1;
      | [] -> failwith "expected outcoins, no outcoins"
    done;
    num_trans := !num_trans - 1;
  done;
  !results
Program.set_output(parse ())
