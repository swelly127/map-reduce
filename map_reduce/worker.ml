open Async.Std
open Protocol

module Make (Job : MapReduce.Job) = struct

  (* see .mli *)
  let run r w =
    let module Request = WorkerRequest(Job) in let module Response = WorkerResponse(Job) in
    Request.receive r >>= (fun x -> match x with
      | `Eof -> return (Response.send w (Response.JobFailed("received eof")))
      | `Ok(a) -> match a with
                  | Request.MapRequest(input) -> Job.map input >>= (fun x -> return (Response.send w (Response.MapResult(x))))
                  | Request.ReduceRequest(k, lst) -> Job.reduce (k, lst) >>= (fun x -> return (Response.send w (Response.ReduceResult(x))))
    )
end

(* see .mli *)
let init port =
  Tcp.Server.create
    ~on_handler_error:`Raise
    (Tcp.on_port port)
    (fun _ r w ->
      Reader.read_line r >>= function
        | `Eof    -> return ()
        | `Ok job -> match MapReduce.get_job job with
          | None -> return ()
          | Some j ->
            let module Job = (val j) in
            let module Worker = Make(Job) in
            Worker.run r w
    )
    >>= fun _ ->
  print_endline "server started";
  print_endline "worker started.";
  print_endline "registered jobs:";
  List.iter print_endline (MapReduce.list_jobs ());
  never ()


