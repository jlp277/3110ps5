open Util;;

let main (args : string array) : unit =
  if Array.length args < 3 then
    print_endline "Usage: grades <filename>"
  else
    let filename = args.(2) in
  let students = load_grades filename in
  let kv_pairs =
    List.rev_map (fun s -> (string_of_int s.id_num, s.course_grades)) students in
  let reduced =
    Map_reduce.map_reduce "grades" "mapper" "reducer" kv_pairs in
  print_reduced_documents reduced in
main Sys.argv