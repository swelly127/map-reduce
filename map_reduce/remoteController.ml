open Async.Std

let addresses = ref []

let init addrs =
	List.map Tcp.to_host_and_port addrs

module Make (Job : MapReduce.Job) = struct
  let map_reduce inputs =
    failwith "Nowhere special."

end
