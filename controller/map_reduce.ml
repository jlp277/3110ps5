open Util
open Worker_manager

let work_retries = 2
let todo = Hashtbl.create 256
let results = Hashtbl.create 256
let task_failures = Hashtbl.create 256
let t_mutex = Mutex.create ()
let r_mutex = Mutex.create ()
let f_mutex = Mutex.create ()

let combined = Hashtbl.create 256

(*globals for reduce*)
let r_todo = Hashtbl.create 256
let r_results = Hashtbl.create 256
let r_task_failures = Hashtbl.create 256
let rt_mutex = Mutex.create ()
let rr_mutex = Mutex.create ()
let rf_mutex = Mutex.create ()

let hash_print table = 
  let tablestring = Hashtbl.fold (fun k v init -> "("^(Util.marshal k)^", "^(Util.marshal v)^") "^init) table "" in
  print_endline tablestring
  
let map kv_pairs map_filename : (string * string) list = 
  let m_pool = Thread_pool.create 20 in
  let manager = Worker_manager.initialize_mappers map_filename in
  let rec assign () : (string * string) list =
    let map_pair (k,v) () = (
      let mapper = Worker_manager.pop_worker manager in
      match Worker_manager.map mapper k v with
      | Some l ->
        Mutex.lock r_mutex;
        Mutex.lock t_mutex;
        if Hashtbl.mem todo (k,v) then (
          Worker_manager.push_worker manager mapper;
          List.iter (fun (k,v) -> Hashtbl.add results k v) l;
          Hashtbl.remove todo (k,v);
          (* print_endline (string_of_int (Hashtbl.length todo)); *)
          Mutex.unlock t_mutex;
          Mutex.unlock r_mutex; )
        else (
          Worker_manager.push_worker manager mapper;
          Mutex.unlock t_mutex;
          Mutex.unlock r_mutex;
          () )
      | None -> 
        Worker_manager.push_worker manager mapper; ) in
    Mutex.lock t_mutex;
    let len = Hashtbl.length todo in
    Mutex.unlock t_mutex;
    if len = 0 then (
      Thread_pool.destroy m_pool;
      Worker_manager.clean_up_workers manager;
      Hashtbl.fold (fun k v init -> (k,v)::init) results []; )
    else (
      Thread.delay 0.1;
      Hashtbl.iter (
        fun (k,v) v' ->
          Thread_pool.add_work (map_pair (k,v)) m_pool; ) todo;
      assign () ) in
  List.iter (fun (k,v) -> Hashtbl.add todo (k,v) "") kv_pairs;
  assign ()

let combine kv_pairs : (string * string list) list = 
  let build_combtable (k,v) =
    if Hashtbl.mem combined k then
      Hashtbl.replace combined k (v::(Hashtbl.find combined k))
    else
      Hashtbl.add combined k [v] in
  List.iter build_combtable kv_pairs;
  Hashtbl.fold (fun k v init -> (k,v)::init) combined []

let reduce kvs_pairs reduce_filename : (string * string list) list =
  let r_pool = Thread_pool.create 20 in
  let manager = Worker_manager.initialize_reducers reduce_filename in
  let rec assign () =
    let reduce_pair (k,v) () = (
      let reducer = Worker_manager.pop_worker manager in
      match Worker_manager.reduce reducer k v with
      | Some l ->
          Mutex.lock rr_mutex;
          Mutex.lock rt_mutex;
          if Hashtbl.mem r_todo (k,v) then (
            Worker_manager.push_worker manager reducer;
            List.iter (fun s -> Hashtbl.add r_results k s) l;
            Hashtbl.remove r_todo (k,v);
            (* print_endline (string_of_int (Hashtbl.length r_todo)); *)
            Mutex.unlock rt_mutex;
            Mutex.unlock rr_mutex; )
          else (
            Worker_manager.push_worker manager reducer;
            Mutex.unlock rt_mutex;
            Mutex.unlock rr_mutex;
            () )
      | None ->
        Worker_manager.push_worker manager reducer; ) in
    Mutex.lock rt_mutex;
    let len = Hashtbl.length r_todo in
    Mutex.unlock rt_mutex;
    if len = 0 then (
      Thread_pool.destroy r_pool;
      Worker_manager.clean_up_workers manager;
      Hashtbl.fold (fun k v init -> (k,[v])::init) r_results [] )
    else (
      Thread.delay 0.1;
      Hashtbl.iter (
        fun (k,v) v' ->
          Thread_pool.add_work (reduce_pair (k, v)) r_pool; ) r_todo;
      assign () ) in
  List.iter (fun (k,v) -> Hashtbl.add r_todo (k,v) "") kvs_pairs;
  assign ()


let map_reduce app_name mapper_name reducer_name kv_pairs =
  let map_filename    = Printf.sprintf "apps/%s/%s.ml" app_name mapper_name  in
  let reduce_filename = Printf.sprintf "apps/%s/%s.ml" app_name reducer_name in
  let mapped   = map kv_pairs map_filename in
  let combined = combine mapped in
  let reduced  = reduce combined reduce_filename in
  reduced

