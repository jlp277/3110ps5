open Util
open Worker_manager

let mutex = Mutex.create ()

(* map tables *)
let todo = Hashtbl.create 256
let results = Hashtbl.create 256
(* combine table *)
let combined = Hashtbl.create 256
(* reduce tables *)
let r_todo = Hashtbl.create 256
let r_results = Hashtbl.create 256

let t_fails = Hashtbl.create 256
let r_t_fails = Hashtbl.create 256
let retries = 5

let hash_print table = 
  let tablestring = Hashtbl.fold (fun k v init -> "("^(Util.marshal k)^", "^(Util.marshal v)^") "^init) table "" in
  print_endline tablestring
  
let map kv_pairs map_filename : (string * string) list = 
  let m_pool = Thread_pool.create 20 in
  let manager = Worker_manager.initialize_mappers map_filename in
  let rec assign () : (string * string) list =
    let map_pair (k,v) () =
      let mapper = Worker_manager.pop_worker manager in
      match Worker_manager.map mapper k v with
      | Some l ->
        Mutex.lock mutex;
        if Hashtbl.mem todo (k,v) then (
          Worker_manager.push_worker manager mapper;
          List.iter (fun (k,v) -> Hashtbl.add results k v) l;
          Hashtbl.remove todo (k,v);
          Mutex.unlock mutex; )
        else (
          Worker_manager.push_worker manager mapper;
          Mutex.unlock mutex;
          () )
      | None ->
        Mutex.lock mutex;
        Worker_manager.push_worker manager mapper;
        Hashtbl.replace t_fails (k,v) (Hashtbl.find t_fails (k,v) + 1);
        Mutex.unlock mutex in
    Mutex.lock mutex;
    let len = Hashtbl.length todo in
    Mutex.unlock mutex;
    if len = 0 then (
      Thread_pool.destroy m_pool;
      Worker_manager.clean_up_workers manager;
      Hashtbl.fold (fun k v init -> (k,v)::init) results []; )
    else (
      Thread.delay 0.1;
      Hashtbl.iter (fun (k,v) v' -> 
        if Hashtbl.find t_fails (k,v) > retries then
          ()
        else
          Thread_pool.add_work (map_pair (k,v)) m_pool) todo;
      assign () ) in
  List.iter (fun (k,v) -> Hashtbl.add todo (k,v) "") kv_pairs;
  List.iter (fun (k,v) -> Hashtbl.add t_fails (k,v) 0) kv_pairs;
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
    let reduce_pair (k,v) () =
      let reducer = Worker_manager.pop_worker manager in
      match Worker_manager.reduce reducer k v with
      | Some l ->
        Mutex.lock mutex;
        if Hashtbl.mem r_todo (k,v) then (
          Worker_manager.push_worker manager reducer;
          List.iter (fun s -> Hashtbl.add r_results k s) l;
          Hashtbl.remove r_todo (k,v);
          Mutex.unlock mutex; )
        else (
          Worker_manager.push_worker manager reducer;
          Mutex.unlock mutex;
          () )
      | None ->
        Mutex.lock mutex;
        Worker_manager.push_worker manager reducer;
        Hashtbl.replace r_t_fails (k,v) (Hashtbl.find r_t_fails (k,v) + 1);
        Mutex.unlock mutex in
    Mutex.lock mutex;
    let len = Hashtbl.length r_todo in
    Mutex.unlock mutex;
    if len = 0 then (
      Thread_pool.destroy r_pool;
      Worker_manager.clean_up_workers manager;
      Hashtbl.fold (fun k v init -> (k,[v])::init) r_results [] )
    else (
      Thread.delay 0.1;
      Hashtbl.iter (fun (k,v) v' -> 
        if Hashtbl.find r_t_fails (k,v) > retries then
          ()
        else
          Thread_pool.add_work (reduce_pair (k,v)) r_pool) r_todo;
      assign () ) in
  List.iter (fun (k,v) -> Hashtbl.add r_todo (k,v) "") kvs_pairs;
  List.iter (fun (k,v) -> Hashtbl.add r_t_fails (k,v) 0) kvs_pairs;
  assign ()


let map_reduce app_name mapper_name reducer_name kv_pairs =
  let map_filename    = Printf.sprintf "apps/%s/%s.ml" app_name mapper_name  in
  let reduce_filename = Printf.sprintf "apps/%s/%s.ml" app_name reducer_name in
  let mapped   = map kv_pairs map_filename in
  let combined = combine mapped in
  let reduced  = reduce combined reduce_filename in
  reduced

