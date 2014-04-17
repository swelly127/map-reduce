open Async.Std

type 'a t = 'a Pipe.Reader.t * 'a Pipe.Writer.t

let create () =
  Pipe.create ()

let push q x =
  don't_wait_for (Pipe.write (snd q) x)

let pop q =
  Pipe.read (fst q) >>= fun x -> match x with | `Ok(a) -> return a | _ -> failwith "empty"
