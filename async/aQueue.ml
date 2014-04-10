open Async.Std

type 'a t = a' Pipe.Reader.t * 'a Pipe.Writer.t

let create () =
  Pipe.create ()

let push q x =
  don't_wait_for (Pipe.Writer.write (snd q) x)

let pop  q =
  match Pipe.read (fst q) with | `Eof Deferred.t -> failwith "end of file" | a -> a

