open Async.Std
open AQueue
open Protocol

let addresses = create ()

let init addrs =
  List.iter (fun x -> push addresses x) addrs

module Make (Job : MapReduce.Job) = struct

  exception MapFailed of string
  exception ReduceFailed of string
  exception InfrastructureFailure

  module Request = WorkerRequest(Job)
  module Response = WorkerResponse(Job)

  type 'a worker = 'a Pipe.Reader.t * 'a Pipe.Writer.t

  let reduce (k, vs) =
    pop addresses
    >>= (fun z -> Tcp.connect (Tcp.to_host_and_port (fst z) (snd z)))
    >>= (fun (sock, r, w) ->
        Request.send w (Request.ReduceRequest(k, vs));
        Response.receive r
        >>= (fun x ->
          match x with
            | `Ok(Response.JobFailed(s)) -> raise (ReduceFailed s)
            | `Ok(Response.ReduceResult(out)) -> Socket.shutdown sock `Both; return (k, out)

            | `Eof -> raise (InfrastructureFailure))
      )

    module C = Combiner.Make(Job)

    let handle_result res = match res with
            | `Ok(Response.JobFailed(s)) -> raise (ReduceFailed s)
            | `Ok(Response.ReduceResult(out)) -> Socket.shutdown sock `Both; return (k, out)
            | `Ok(Response.MapResult(lst)) -> Socket.shutdown sock `Both; return lst
            | `Eof -> raise (InfrastructureFailure))


    let map_reduce inputs =
      Deferred.List.map inputs
        (fun x ->
          pop addresses
          >>= (fun z -> Tcp.connect (Tcp.to_host_and_port (fst z) (snd z)))
          >>= (fun (sock, r, w) ->
              Writer.write_line w Job.name;
              Request.send w (Request.MapRequest(x));
              Response.receive r >>= (fun result ->
                match result with
                  | `Ok(Response.JobFailed(s)) -> raise (MapFailed s)
                  | `Ok(Response.MapResult(lst)) -> Socket.shutdown sock `Both; return lst
                  | `Eof -> raise (InfrastructureFailure))))
      >>| List.flatten
      >>| C.combine
      >>= fun l ->
      Deferred.List.map l reduce
end
