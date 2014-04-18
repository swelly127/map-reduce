open Async.Std
open AQueue

let addresses = create ()

let init addrs =
  List.iter (fun x -> push addresses x) addrs

module Make (Job : MapReduce.Job) = struct

  type 'a worker = 'a Pipe.Reader.t * 'a Pipe.Writer.t

    let reduce (k, vs) =
      Job.reduce (k, vs) >>= fun out -> return (k, out)

    module C = Combiner.Make(Job)

    let map_reduce inputs =
      failwith "help me"
    (**Deferred.List.map addresses (fun x -> match try_with
      (Tcp.connect (Tcp.to_host_and_port x)
        >>= (fun lst ->
          Writer.write_line w Job.name;
          Writer.write_line w WorkerRequest.MapRequest(Job.input)
          Reader.read_line r >>= (fun x ->
           (match x with
             | `Eof -> ()
             | `Ok s -> );
           Socket.shutdown sock `Both;
           return ()))

          Deferred.List.map ~how:`Parallel inputs ~Job:
              >>= fun (sock, r, w) -> WorkerRequest.send Job.inpit
            >>| List.flatten
            >>| C.combine
            >>= fun l ->
          Deferred.List.map l reduce
      ) with
      | `Ok(a) -> a | `Err(e) -> throw e;
    )**)


end
