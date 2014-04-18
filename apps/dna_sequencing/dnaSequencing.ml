open Async.Std
open Async_unix

(******************************************************************************)
(** input and output types                                                    *)
(******************************************************************************)

type id = int
type dna_type = Read | Ref

type sequence = {
  id   : id;
  kind : dna_type;
  data : string;
}

(** Indicates a matching subsequence of the given read and reference *)
type result = {
  length   : int;

  read     : id;
  read_off : int;

  ref      : id;
  ref_off  : int;
}

(******************************************************************************)
(** file reading and writing                                                  *)
(******************************************************************************)

(** Convert a line into a sequence *)
let read_sequence line = match Str.split (Str.regexp "@") line with
  | [id; "READ"; seq] -> {id=int_of_string id; kind=Read; data=seq}
  | [id; "REF";  seq] -> {id=int_of_string id; kind=Ref;  data=seq}
  | _ -> failwith "malformed input"

(** Read in the input data *)
let read_files filenames : sequence list Deferred.t =
  if filenames = [] then failwith "No files supplied"
  else
    Deferred.List.map filenames Reader.file_lines
      >>| List.flatten
      >>| List.map read_sequence


(** Print out a single match *)
let print_result result =
  printf "read %i [%i-%i] matches reference %i [%i-%i]\n"
         result.read result.read_off (result.read_off + result.length - 1)
         result.ref  result.ref_off  (result.ref_off  + result.length - 1)

(** Write out the output data *)
let print_results results : unit =
  List.iter print_result results

(******************************************************************************)
(** Dna sequencing jobs                                                       *)
(******************************************************************************)

module Job1 = struct
  type input = sequence
  type key = string
  type inter = id * dna_type * int
  type output = (inter * inter) list

  let name = "dna.job1"

  let map input : (key * inter) list Deferred.t =
    let rec helper start = match (String.length input.data) - start with
      | n when n < 10 -> []
      | _ -> (String.sub input.data start 10, (input.id, input.kind, start))::(helper (start+1))
    in return (helper 0)

  let reduce (key, inters) : output Deferred.t =
    let ref_list = List.filter (fun x -> match x with | (_, Ref, _) -> true | _ -> false) inters in
    let read_list = List.filter (fun x -> if List.mem x ref_list then false else true) inters in
    return (List.concat (List.rev_map (fun x -> List.rev_map (fun y -> x, y) read_list) ref_list))
end

let () = MapReduce.register_job (module Job1)

module Job2 = struct
  type input = Job1.output
  type key = (id * id)
  type inter = (int * int)
  type output = result list

  let name = "dna.job2"

  let map input : (key * inter) list Deferred.t =
    return (List.map (fun (a, b) -> match (a, b) with | (a1, a2, a3), (b1, b2, b3) -> (a1, b1), (a3, b3)) input)

  let reduce (key, inters) : output Deferred.t =
    let sorted =  List.sort (fun a b -> compare (fst a) (fst b)) inters in
    let rec helper lst l = match lst with
      | [] -> []
      | (a, b)::(c, d)::t when a+1=c && b+1=d -> helper ((a+1, b+1)::t) (l+1)
      | (a, b)::t -> {length=(l+10); ref=(fst key); ref_off=(a-l); read=(snd key); read_off=(b-l)}::(helper t 0)
    in return (helper sorted 0)
end

let () = MapReduce.register_job (module Job2)

module App  = struct

  let name = "dna"

  module Make (Controller : MapReduce.Controller) = struct
    module MR1 = Controller(Job1)
    module MR2 = Controller(Job2)

    let run (input : sequence list) : result list Deferred.t =
      return input >>= MR1.map_reduce
      >>| List.map snd
      >>= MR2.map_reduce
      >>| List.map snd
      >>| List.flatten

    let main args =
      read_files args
        >>= run
        >>| print_results
  end
end

let () = MapReduce.register_app (module App)


