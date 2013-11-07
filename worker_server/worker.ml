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
              Mutex.unlock m_mutex;
              handle_request client )
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
              Mutex.unlock r_mutex;
              handle_request client ) 
            else
              () ) 
          | None, error -> 
            let _ = send_response client error in () )
        | MapRequest (id, k, v) ->
          (*TODO What is key k for?*)
          if (Hashtbl.mem mappers id) then
            match Program.run id v with
            |Some result -> (
              (*TODO Should we remove the id from the hashtable somewhere in here?*)
              if send_response client (MapResults(id,result)) then
                handle_request client
              else
                () )
            |None ->
              (*TODO should we recursively call here as well?*)
              let _ = send_response client (RuntimeError(id, "No Result")) in ()
          else
            let _ = send_response client (InvalidWorker(id)) in ()
        | ReduceRequest (id, k, v) -> 
          (*TODO What is key k for?*)
          if (Hashtbl.mem reducers id) then
            match Program.run id v with
            |Some result -> (
              (*TODO Should we remove the id from the hashtable somewhere in here?*)
              if send_response client (ReduceResults(id,result)) then
                handle_request client
              else
                () )
            |None ->
              (*TODO should we recursively call here as well?*)
              let _ = send_response client (RuntimeError(id, "No Result")) in ()
          else
            let _ = send_response client (InvalidWorker(id)) in ()
        )
      end
  | None ->
      Connection.close client;
      print_endline "Connection lost while waiting for request."
