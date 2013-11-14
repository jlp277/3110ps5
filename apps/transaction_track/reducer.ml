(* use comp in -> List.sort comp values *)
let comp (key,amt) (key',amt') = 
  if key > key' then 1
  else if key < key' then -1
  else 0 in