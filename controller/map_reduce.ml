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

(* TODO implement these *)
let map kv_pairs map_filename : (string * string) list = 
  (* create threadpool *)
  let m_pool = Thread_pool.create 20 in
  let manager = Worker_manager.initialize_mappers map_filename in
  let rec assign () : (string * string) list =
    let map_pair (k,v) () = (
      let mapper = Worker_manager.pop_worker manager in
      match Worker_manager.map mapper k v with
      | Some l ->
        Worker_manager.push_worker manager mapper;
        List.iter (fun (k,v) -> Mutex.lock r_mutex; Hashtbl.add results k v; Mutex.unlock r_mutex;) l;
        Mutex.lock t_mutex;
        Hashtbl.remove todo k;
        Mutex.unlock t_mutex;
      | None ->
        Mutex.lock f_mutex;
        if Hashtbl.mem task_failures (k,v) then
          Hashtbl.replace task_failures (k,v) ((Hashtbl.find task_failures (k,v)) + 1)
        else
          Hashtbl.add task_failures (k,v) 1;
        Mutex.unlock f_mutex;) in
    if Hashtbl.length todo = 0 then
      (Worker_manager.clean_up_workers manager;
      Thread_pool.destroy m_pool;
      Hashtbl.fold (fun k v init -> (k,v)::init) results [])
    else
      (Thread.delay 0.1;
      (* marshalling may not be type safe. check if this implementation is correct *)
      Hashtbl.iter (
        fun k v ->
          if Hashtbl.find task_failures (k,v) > work_retries then
            Thread_pool.add_work (map_pair (k, v)) m_pool
          else
            Mutex.lock t_mutex;
            Hashtbl.remove todo k;
            Mutex.unlock t_mutex;
            Mutex.lock f_mutex;
            Hashtbl.replace task_failures (k,v) 0; ) todo;
      Mutex.unlock f_mutex;
      assign ()) in
  List.iter (fun (k,v) -> (Hashtbl.add todo k v)) kv_pairs;
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
  failwith "The only thing necessary for evil to triumph is for good men to do nothing"

let map_reduce app_name mapper_name reducer_name kv_pairs =
  let map_filename    = Printf.sprintf "apps/%s/%s.ml" app_name mapper_name  in
  let reduce_filename = Printf.sprintf "apps/%s/%s.ml" app_name reducer_name in
  let mapped   = map kv_pairs map_filename in
  let combined = combine mapped in
  let reduced  = reduce combined reduce_filename in
  reduced

