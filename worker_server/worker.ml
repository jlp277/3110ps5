open Protocol

let mappers = Hashtbl.create 256
let reducers = Hashtbl.create 256

let m_mutex = Mutex.create ()
let r_mutex = Mutex.create ()

let send_response client response =
  let success = Connection.output client response in
    (if not success then
      (Connection.close client;
       print_endline "Connection lost before response could be sent.")
    else ());
    success

let rec handle_request client =
  match Connection.input client with
  | Some v ->
      begin
        (match v with
        | InitMapper source -> 
          (match Program.build source with
          | Some id, _ -> (
            if send_response client (Mapper(Some id, "")) then (
              Mutex.lock m_mutex;
              Hashtbl.add mappers id "";
              Mutex.unlock m_mutex )
            else
              () )
          | None, error ->
            let _ = send_response client error in () )
        | InitReducer source -> 
          (match Program.build source with
          | Some id, _ -> (
            if send_response client (Reducer(Some id, "")) then (
              Mutex.lock r_mutex;
              Hashtbl.add reducers id "";
              Mutex.unlock r_mutex )
            else
              () ) 
          | None, error -> 
            let _ = send_response client error in () )
        | MapRequest (id, k, v) -> 
          failwith "You won't go unrewarded."
        | ReduceRequest (id, k, v) -> 
          failwith "Really? In that case, just tell me what you need.")
      end
  | None ->
      Connection.close client;
      print_endline "Connection lost while waiting for request."