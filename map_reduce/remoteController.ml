open Async.Std

let addresses = ref []

let init addrs =
	addresses := addrs

module Make (Job : MapReduce.Job) = struct

	type worker = Reader t * Writer t

  	let reduce (k, vs) =
    	Job.reduce (k, vs) >>= fun out -> return (k, out)

  	module C = Combiner.Make(Job)

  	let map_reduce inputs =


	Deferred.List.map addresses (fun x ->
	 	try_with
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
			)

	 	if error then throw something
	 	)


end
