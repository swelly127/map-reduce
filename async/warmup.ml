open Async.Std

let fork d f1 f2 =
  ignore (Deferred.both (d >>= f1) (d >>= f2))

let deferred_map l f =
  return (List.map (fun x -> match Deferred.peek (x >>= f) with |Some x -> x) l)


