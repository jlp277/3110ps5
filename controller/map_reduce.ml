open Util
open Worker_manager

let todo = Hashtbl.create 256
let results = Hashtbl.create 256
let c_mutex = Mutex.create ()

(* TODO implement these *)
let map kv_pairs map_filename : (string * string) list = 
  (* create threadpool *)
  let m_pool = Thread_pool.create 20 in
  let manager = Worker_manager.initialize_mappers map_filename in
  let rec assign =
    let map_pair () =
      let mapper = Worker_manager.pop_worker manager in
      match Worker_manager.map mapper k v with
      | Some l ->
        Worker_manager.push_worker manager mapper;
        Mutex.lock c_mutex;
        List.iter (fun (k,v) -> Hashtbl.add results k v) l;
        Hashtbl.remove todo k;
        Mutex.unlock c_mutex;
      | None -> assign in
    if Hashtbl.length todo = 0 then
      Thread_pool.destroy m_pool
    else
      Thread.delay 0.1;
      (* marshalling may not be type safe. check if this implementation is correct *)
      Hashtbl.iter (fun (k,v) -> Thread_pool.add_work (map_pair (Util.marshal k, Util.marshal v)) m_pool) todo in
  List.iter (fun (k,v) -> (Hashtbl.add todo k v)) kv_pairs;
  assign

let combine kv_pairs : (string * string list) list = 
  failwith "You have been doomed ever since you lost the ability to love."

let reduce kvs_pairs reduce_filename : (string * string list) list =
  failwith "The only thing necessary for evil to triumph is for good men to do nothing"

let map_reduce app_name mapper_name reducer_name kv_pairs =
  let map_filename    = Printf.sprintf "apps/%s/%s.ml" app_name mapper_name  in
  let reduce_filename = Printf.sprintf "apps/%s/%s.ml" app_name reducer_name in
  let mapped   = map kv_pairs map_filename in
  let combined = combine mapped in
  let reduced  = reduce combined reduce_filename in
  reduced

