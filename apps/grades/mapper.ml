open Util;;

let (key, value) = Program.get_input() in
Program.set_output (
  List.map (
    fun s -> match Util.split_spaces s with
      | [course;grade] -> (course,grade)
      | _ -> failwith "not a [course;grade] list" )
  (Util.split_to_class_lst value)
)